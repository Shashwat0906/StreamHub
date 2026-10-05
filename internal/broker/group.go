package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

// Group states.
const (
	stateEmpty     = "Empty"
	statePreparing = "PreparingRebalance"
	stateStable    = "Stable"
)

const (
	strategyRange      = "range"
	strategyRoundRobin = "roundrobin"
)

type joinResult struct {
	resp *protocol.JoinGroupResponse
}

type member struct {
	id               string
	clientID         string
	topics           []string
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
	lastHeartbeat    time.Time
	// join is non-nil while this member has a JoinGroup request waiting
	// for the current rebalance to finish.
	join       chan joinResult
	assignment []protocol.TopicPartitions
}

type group struct {
	id                string
	partition         int32 // __consumer_offsets partition that owns this group
	state             string
	generation        int32
	strategy          string
	members           map[string]*member
	rebalanceDeadline time.Time
	initialDelayUntil time.Time
}

type offsetEntry struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata,omitempty"`
}

// coordinator implements consumer groups for the groups whose offsets
// partition this broker leads.
//
// Membership lives only in memory (like Kafka): if the coordinator moves,
// members get NOT_COORDINATOR, find the new one and rejoin. Committed
// offsets are records in the replicated __consumer_offsets topic, written
// with acks=all, so they survive coordinator failure; a new coordinator
// rebuilds its cache by reading the partition when it becomes leader.
type coordinator struct {
	b *Broker

	mu      sync.Mutex
	groups  map[string]*group
	offsets map[string]map[tp]offsetEntry // group -> partition -> committed
	loaded  map[int32]int32               // offsets partition -> leader epoch it was loaded at
	loading map[int32]bool
}

func newCoordinator(b *Broker) *coordinator {
	return &coordinator{
		b:       b,
		groups:  map[string]*group{},
		offsets: map[string]map[tp]offsetEntry{},
		loaded:  map[int32]int32{},
		loading: map[int32]bool{},
	}
}

// groupPartition maps a group to its __consumer_offsets partition.
func groupPartition(group string, n int32) int32 {
	h := fnv.New32a()
	h.Write([]byte(group))
	return int32((h.Sum32() & 0x7fffffff) % uint32(n))
}

// ---------------------------------------------------------------- ownership

func (c *coordinator) offsetsPartitions() int32 {
	if t, ok := c.b.meta.Topic(OffsetsTopic); ok {
		return int32(len(t.Partitions))
	}
	return 0
}

// owner returns the offsets partition for group and an error code if this
// broker cannot coordinate it right now.
func (c *coordinator) owner(groupID string) (int32, protocol.ErrorCode) {
	n := c.offsetsPartitions()
	if n == 0 {
		return -1, protocol.ErrCoordinatorNotAvailable
	}
	part := groupPartition(groupID, n)
	p := c.b.replicas.get(OffsetsTopic, part)
	if p == nil {
		return part, protocol.ErrNotCoordinator
	}
	r, _, epoch, _, _ := p.Snapshot()
	if r != roleLeader {
		return part, protocol.ErrNotCoordinator
	}
	c.mu.Lock()
	loadedEpoch, ok := c.loaded[part]
	c.mu.Unlock()
	if !ok || loadedEpoch != epoch {
		return part, protocol.ErrCoordinatorNotAvailable // still loading
	}
	return part, protocol.ErrNone
}

// onLeadershipChange is called by the replica manager when this broker
// gains or loses leadership of an offsets partition.
func (c *coordinator) onLeadershipChange(p *Partition, leader bool, epoch int32) {
	if leader {
		go c.load(p, epoch)
		return
	}
	c.unload(p.ID)
}

// load rebuilds the committed-offset cache from the partition log.
func (c *coordinator) load(p *Partition, epoch int32) {
	c.mu.Lock()
	if c.loading[p.ID] {
		c.mu.Unlock()
		return
	}
	c.loading[p.ID] = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.loading, p.ID)
		c.mu.Unlock()
	}()

	start := time.Now()
	recs, err := p.ReadAll()
	if err != nil {
		c.b.logger.Error("offsets load failed", "partition", p.ID, "err", err)
		return
	}
	loaded := map[string]map[tp]offsetEntry{}
	for _, r := range recs {
		g, key, ok := decodeOffsetKey(r.Key)
		if !ok {
			continue
		}
		var v offsetEntry
		if json.Unmarshal(r.Value, &v) != nil {
			continue
		}
		if loaded[g] == nil {
			loaded[g] = map[tp]offsetEntry{}
		}
		loaded[g][key] = v
	}
	if r, _, e, _, _ := p.Snapshot(); r != roleLeader || e != epoch {
		return // lost leadership while loading
	}
	c.mu.Lock()
	for g, m := range loaded {
		c.offsets[g] = m
	}
	c.loaded[p.ID] = epoch
	c.mu.Unlock()
	c.b.logger.Info("coordinator loaded offsets partition", "partition", p.ID, "records", len(recs),
		"groups", len(loaded), "took", time.Since(start).String())
}

// unload drops groups owned by an offsets partition we no longer lead.
func (c *coordinator) unload(part int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.loaded[part]; !ok {
		return
	}
	delete(c.loaded, part)
	for id, g := range c.groups {
		if g.partition != part {
			continue
		}
		for _, m := range g.members {
			if m.join != nil {
				m.join <- joinResult{&protocol.JoinGroupResponse{Err: protocol.ErrNotCoordinator}}
				m.join = nil
			}
		}
		delete(c.groups, id)
		delete(c.offsets, id)
	}
	c.b.logger.Info("coordinator unloaded offsets partition", "partition", part)
}

// ---------------------------------------------------------------- membership

func newMemberID(clientID string) string {
	if clientID == "" {
		clientID = "consumer"
	}
	return fmt.Sprintf("%s-%08x%08x", clientID, rand.Uint32(), rand.Uint32())
}

func (c *coordinator) join(ctx context.Context, req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	if req.Group == "" {
		return &protocol.JoinGroupResponse{Err: protocol.ErrInvalidRequest}
	}
	part, code := c.owner(req.Group)
	if code != protocol.ErrNone {
		return &protocol.JoinGroupResponse{Err: code}
	}
	strategy := req.Strategy
	if strategy == "" {
		strategy = strategyRange
	}
	if strategy != strategyRange && strategy != strategyRoundRobin {
		return &protocol.JoinGroupResponse{Err: protocol.ErrInvalidRequest}
	}
	session := time.Duration(req.SessionTimeoutMs) * time.Millisecond
	if session <= 0 {
		session = 10 * time.Second
	}
	rebalance := time.Duration(req.RebalanceTimeoutMs) * time.Millisecond
	if rebalance <= 0 {
		rebalance = 30 * time.Second
	}

	now := time.Now()
	c.mu.Lock()
	g := c.groups[req.Group]
	if g == nil {
		g = &group{id: req.Group, partition: part, state: stateEmpty, members: map[string]*member{}}
		c.groups[req.Group] = g
	}
	var m *member
	if req.MemberID == "" {
		m = &member{id: newMemberID(req.ClientID)}
		g.members[m.id] = m
		if len(g.members) == 1 {
			g.strategy = strategy
			// Wait briefly so consumers starting together land in one
			// rebalance instead of N consecutive ones.
			g.initialDelayUntil = now.Add(c.b.cfg.GroupInitialDelay)
		}
		c.b.logger.Info("member joined group", "group", g.id, "member", m.id)
	} else {
		m = g.members[req.MemberID]
		if m == nil {
			c.mu.Unlock()
			return &protocol.JoinGroupResponse{Err: protocol.ErrUnknownMemberID}
		}
	}
	sameSubscription := slices.Equal(sortedStrings(m.topics), sortedStrings(req.Topics))
	m.clientID = req.ClientID
	m.topics = slices.Clone(req.Topics)
	m.sessionTimeout = session
	m.rebalanceTimeout = rebalance
	m.lastHeartbeat = now

	// A known member rejoining a stable group with an unchanged
	// subscription gets its current assignment back without disturbing
	// everyone else.
	if g.state == stateStable && req.MemberID != "" && sameSubscription {
		resp := c.joinResponse(g, m)
		c.mu.Unlock()
		return resp
	}

	if m.join != nil {
		m.join <- joinResult{&protocol.JoinGroupResponse{Err: protocol.ErrRebalanceInProgress}}
	}
	ch := make(chan joinResult, 1)
	m.join = ch
	c.triggerRebalanceLocked(g, now, "join from "+m.id)
	c.maybeCompleteLocked(g, now)
	c.mu.Unlock()

	timer := time.NewTimer(rebalance + 5*time.Second)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.resp
	case <-ctx.Done():
	case <-timer.C:
	}
	c.mu.Lock()
	if m.join == ch {
		m.join = nil
	}
	c.mu.Unlock()
	return &protocol.JoinGroupResponse{Err: protocol.ErrRebalanceInProgress}
}

func (c *coordinator) joinResponse(g *group, m *member) *protocol.JoinGroupResponse {
	return &protocol.JoinGroupResponse{
		Generation: g.generation, MemberID: m.id, Strategy: g.strategy,
		Members: int32(len(g.members)), Assignment: m.assignment,
	}
}

func (c *coordinator) triggerRebalanceLocked(g *group, now time.Time, reason string) {
	if g.state == statePreparing {
		return
	}
	g.state = statePreparing
	var maxTimeout time.Duration
	for _, m := range g.members {
		maxTimeout = max(maxTimeout, m.rebalanceTimeout)
	}
	g.rebalanceDeadline = now.Add(maxTimeout)
	c.b.logger.Info("rebalance started", "group", g.id, "generation", g.generation, "members", len(g.members), "reason", reason)
}

// maybeCompleteLocked finishes a rebalance once every member has rejoined
// (or the rebalance timeout expired; members that did not rejoin in time
// are removed).
func (c *coordinator) maybeCompleteLocked(g *group, now time.Time) {
	if g.state != statePreparing {
		return
	}
	allJoined := true
	for _, m := range g.members {
		if m.join == nil {
			allJoined = false
		}
	}
	timedOut := now.After(g.rebalanceDeadline)
	if !(allJoined && !now.Before(g.initialDelayUntil)) && !timedOut {
		return
	}
	if timedOut {
		for id, m := range g.members {
			if m.join == nil {
				c.b.logger.Info("member removed: did not rejoin before rebalance timeout", "group", g.id, "member", id)
				delete(g.members, id)
			}
		}
	}
	g.generation++
	if len(g.members) == 0 {
		g.state = stateEmpty
		return
	}

	// Compute the assignment over the union of subscribed topics.
	partitions := map[string]int32{}
	var ams []AssignMember
	for _, m := range g.members {
		ams = append(ams, AssignMember{ID: m.id, Topics: m.topics})
		for _, t := range m.topics {
			if tm, ok := c.b.meta.Topic(t); ok {
				partitions[t] = int32(len(tm.Partitions))
			}
		}
	}
	var assignment map[string][]protocol.TopicPartitions
	if g.strategy == strategyRoundRobin {
		assignment = AssignRoundRobin(ams, partitions)
	} else {
		assignment = AssignRange(ams, partitions)
	}
	g.state = stateStable
	for id, m := range g.members {
		m.assignment = assignment[id]
		m.lastHeartbeat = now
		m.join <- joinResult{c.joinResponse(g, m)}
		m.join = nil
	}
	c.b.metrics.rebalances.Inc()
	c.b.logger.Info("rebalance completed", "group", g.id, "generation", g.generation,
		"members", len(g.members), "strategy", g.strategy)
}

func (c *coordinator) removeMemberLocked(g *group, id string, now time.Time, reason string) {
	m := g.members[id]
	if m == nil {
		return
	}
	if m.join != nil {
		m.join <- joinResult{&protocol.JoinGroupResponse{Err: protocol.ErrUnknownMemberID}}
	}
	delete(g.members, id)
	c.b.logger.Info("member left group", "group", g.id, "member", id, "reason", reason)
	if len(g.members) == 0 {
		// Bump the generation so commits from the departed generation are
		// rejected, then sit empty.
		g.generation++
		g.state = stateEmpty
		return
	}
	c.triggerRebalanceLocked(g, now, reason)
	c.maybeCompleteLocked(g, now)
}

func (c *coordinator) heartbeat(req *protocol.HeartbeatRequest) *protocol.SimpleResponse {
	if _, code := c.owner(req.Group); code != protocol.ErrNone {
		return &protocol.SimpleResponse{Err: code}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[req.Group]
	if g == nil || g.members[req.MemberID] == nil {
		return &protocol.SimpleResponse{Err: protocol.ErrUnknownMemberID}
	}
	g.members[req.MemberID].lastHeartbeat = time.Now()
	if req.Generation != g.generation {
		return &protocol.SimpleResponse{Err: protocol.ErrIllegalGeneration}
	}
	if g.state == statePreparing {
		// Tell the member to commit what it has processed and rejoin.
		return &protocol.SimpleResponse{Err: protocol.ErrRebalanceInProgress}
	}
	return &protocol.SimpleResponse{}
}

func (c *coordinator) leave(req *protocol.LeaveGroupRequest) *protocol.SimpleResponse {
	if _, code := c.owner(req.Group); code != protocol.ErrNone {
		return &protocol.SimpleResponse{Err: code}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[req.Group]
	if g == nil || g.members[req.MemberID] == nil {
		return &protocol.SimpleResponse{Err: protocol.ErrUnknownMemberID}
	}
	c.removeMemberLocked(g, req.MemberID, time.Now(), "leave request")
	return &protocol.SimpleResponse{}
}

// tick expires members whose session ran out and finishes rebalances
// whose timeout passed.
func (c *coordinator) tick() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, g := range c.groups {
		for id, m := range g.members {
			if m.join == nil && now.Sub(m.lastHeartbeat) > m.sessionTimeout {
				c.removeMemberLocked(g, id, now, "session timeout")
			}
		}
		c.maybeCompleteLocked(g, now)
	}
}

// ---------------------------------------------------------------- offsets

func encodeOffsetKey(group, topic string, partition int32) []byte {
	return []byte("o\x00" + group + "\x00" + topic + "\x00" + strconv.Itoa(int(partition)))
}

func decodeOffsetKey(b []byte) (string, tp, bool) {
	parts := strings.Split(string(b), "\x00")
	if len(parts) != 4 || parts[0] != "o" {
		return "", tp{}, false
	}
	p, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", tp{}, false
	}
	return parts[1], tp{parts[2], int32(p)}, true
}

func (c *coordinator) commit(ctx context.Context, req *protocol.OffsetCommitRequest) *protocol.SimpleResponse {
	part, code := c.owner(req.Group)
	if code != protocol.ErrNone {
		return &protocol.SimpleResponse{Err: code}
	}
	c.mu.Lock()
	g := c.groups[req.Group]
	switch {
	case req.MemberID == "" && req.Generation < 0:
		// "Simple" commit from a consumer not using group membership:
		// only allowed while no live members own the partitions.
		if g != nil && len(g.members) > 0 {
			c.mu.Unlock()
			return &protocol.SimpleResponse{Err: protocol.ErrIllegalGeneration, ErrMsg: "group has active members"}
		}
	case g == nil || g.members[req.MemberID] == nil:
		c.mu.Unlock()
		return &protocol.SimpleResponse{Err: protocol.ErrUnknownMemberID}
	case req.Generation != g.generation:
		// Zombie fencing: a consumer from an older generation (e.g. one
		// that was kicked out after a long GC pause) cannot overwrite
		// offsets committed by the current owner of the partition.
		c.mu.Unlock()
		return &protocol.SimpleResponse{Err: protocol.ErrIllegalGeneration}
	}
	c.mu.Unlock()
	if len(req.Offsets) == 0 {
		return &protocol.SimpleResponse{}
	}

	now := time.Now().UnixMilli()
	recs := make([]storage.Record, 0, len(req.Offsets))
	for _, o := range req.Offsets {
		v, _ := json.Marshal(offsetEntry{Offset: o.Offset, Metadata: o.Metadata})
		recs = append(recs, storage.Record{Key: encodeOffsetKey(req.Group, o.Topic, o.Partition), Value: v, Timestamp: now})
	}
	resp := c.b.produce(ctx, &protocol.ProduceRequest{
		Acks: protocol.AcksAll, TimeoutMs: 10000, Topic: OffsetsTopic, Partition: part,
		ProducerID: -1, BaseSequence: -1, Records: recs,
	})
	if resp.Err != protocol.ErrNone {
		code := protocol.ErrCoordinatorNotAvailable
		if resp.Err == protocol.ErrNotLeader {
			code = protocol.ErrNotCoordinator
		}
		return &protocol.SimpleResponse{Err: code, ErrMsg: "offset write failed: " + resp.Err.String()}
	}
	c.mu.Lock()
	if c.offsets[req.Group] == nil {
		c.offsets[req.Group] = map[tp]offsetEntry{}
	}
	for _, o := range req.Offsets {
		c.offsets[req.Group][tp{o.Topic, o.Partition}] = offsetEntry{Offset: o.Offset, Metadata: o.Metadata}
	}
	c.mu.Unlock()
	return &protocol.SimpleResponse{}
}

func (c *coordinator) fetchOffsets(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	if _, code := c.owner(req.Group); code != protocol.ErrNone {
		return &protocol.OffsetFetchResponse{Err: code}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	committed := c.offsets[req.Group]
	resp := &protocol.OffsetFetchResponse{}
	if len(req.Partitions) == 0 {
		keys := make([]tp, 0, len(committed))
		for k := range committed {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].topic != keys[j].topic {
				return keys[i].topic < keys[j].topic
			}
			return keys[i].partition < keys[j].partition
		})
		for _, k := range keys {
			v := committed[k]
			resp.Offsets = append(resp.Offsets, protocol.CommitOffset{Topic: k.topic, Partition: k.partition, Offset: v.Offset, Metadata: v.Metadata})
		}
		return resp
	}
	for _, tps := range req.Partitions {
		for _, p := range tps.Partitions {
			o := protocol.CommitOffset{Topic: tps.Topic, Partition: p, Offset: -1}
			if v, ok := committed[tp{tps.Topic, p}]; ok {
				o.Offset, o.Metadata = v.Offset, v.Metadata
			}
			resp.Offsets = append(resp.Offsets, o)
		}
	}
	return resp
}

func (c *coordinator) listGroups() *protocol.ListGroupsResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp := &protocol.ListGroupsResponse{}
	seen := map[string]bool{}
	for id, g := range c.groups {
		resp.Groups = append(resp.Groups, protocol.GroupListing{Group: id, State: g.state})
		seen[id] = true
	}
	// Groups with committed offsets but no live members.
	for id := range c.offsets {
		if !seen[id] {
			resp.Groups = append(resp.Groups, protocol.GroupListing{Group: id, State: stateEmpty})
		}
	}
	sort.Slice(resp.Groups, func(i, j int) bool { return resp.Groups[i].Group < resp.Groups[j].Group })
	return resp
}

func (c *coordinator) describe(req *protocol.DescribeGroupRequest) *protocol.DescribeGroupResponse {
	if _, code := c.owner(req.Group); code != protocol.ErrNone {
		return &protocol.DescribeGroupResponse{Err: code}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[req.Group]
	if g == nil {
		if _, ok := c.offsets[req.Group]; ok {
			return &protocol.DescribeGroupResponse{State: stateEmpty}
		}
		return &protocol.DescribeGroupResponse{Err: protocol.ErrGroupIDNotFound}
	}
	resp := &protocol.DescribeGroupResponse{State: g.state, Strategy: g.strategy, Generation: g.generation}
	for _, id := range sortedKeys(g.members) {
		m := g.members[id]
		resp.Members = append(resp.Members, protocol.GroupMemberInfo{MemberID: m.id, ClientID: m.clientID, Assignment: m.assignment})
	}
	return resp
}

// ---------------------------------------------------------------- FindCoordinator

func (b *Broker) handleFindCoordinator(ctx context.Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	t, ok := b.meta.Topic(OffsetsTopic)
	if !ok {
		b.ensureOffsetsTopic(ctx)
		return &protocol.FindCoordinatorResponse{Err: protocol.ErrCoordinatorNotAvailable, NodeID: -1}
	}
	part := groupPartition(req.Group, int32(len(t.Partitions)))
	pm, _ := b.meta.Partition(OffsetsTopic, part)
	if pm.Leader == metadata.NoLeader {
		return &protocol.FindCoordinatorResponse{Err: protocol.ErrCoordinatorNotAvailable, NodeID: -1}
	}
	addr, ok := b.brokerAddr(pm.Leader)
	if !ok {
		return &protocol.FindCoordinatorResponse{Err: protocol.ErrCoordinatorNotAvailable, NodeID: -1}
	}
	return &protocol.FindCoordinatorResponse{NodeID: pm.Leader, Addr: addr}
}

// ensureOffsetsTopic creates __consumer_offsets on first use. Its
// replication factor is min(configured, live brokers); retention is
// disabled because StreamHub has no log compaction (see PLAN.md).
func (b *Broker) ensureOffsetsTopic(ctx context.Context) {
	live := len(b.meta.LiveBrokers())
	if live == 0 {
		return
	}
	rf := min(int(b.cfg.OffsetsReplication), live)
	configs := map[string]string{
		"retention.ms":        "-1",
		"retention.bytes":     "-1",
		"min.insync.replicas": strconv.Itoa(min(b.cfg.MinInsyncReplicas, rf)),
	}
	req := &protocol.CreateTopicRequest{Name: OffsetsTopic, Partitions: b.cfg.OffsetsPartitions, ReplicationFactor: int16(rf), Configs: configs}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if b.proposer.IsLeader() {
		res := b.createTopic(cctx, req.Name, req.Partitions, req.ReplicationFactor, req.Configs)
		if res.Err != protocol.ErrNone && res.Err != protocol.ErrTopicAlreadyExists {
			b.logger.Warn("creating offsets topic failed", "err", res.Err.String(), "msg", res.Msg)
		}
		return
	}
	if addr, ok := b.brokerAddr(b.proposer.LeaderID()); ok {
		var resp protocol.SimpleResponse
		b.pool.Call(cctx, addr, protocol.APICreateTopic, req, &resp)
	}
}

func sortedStrings(v []string) []string {
	c := slices.Clone(v)
	sort.Strings(c)
	return c
}
