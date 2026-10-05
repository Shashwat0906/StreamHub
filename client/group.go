package client

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// GroupConfig configures a GroupConsumer.
type GroupConfig struct {
	Group  string
	Topics []string
	// Strategy is "range" (default) or "roundrobin".
	Strategy string
	// SessionTimeout: the coordinator removes us if it hears no heartbeat
	// for this long (default 10s).
	SessionTimeout time.Duration
	// RebalanceTimeout: how long the coordinator waits for every member to
	// rejoin during a rebalance (default 30s).
	RebalanceTimeout time.Duration
	// HeartbeatInterval defaults to SessionTimeout/3.
	HeartbeatInterval time.Duration
	Fetch             FetchConfig
}

// GroupConsumer consumes a set of topics as a member of a consumer group:
// partitions are divided among members by the group coordinator, and
// progress is stored as committed offsets.
//
// Delivery is at-least-once. Records returned by Poll are considered
// processed once Poll is called again (or Commit is called); positions of
// processed records are committed on Commit, before every rebalance, and on
// Close. A crash between processing and commit means those records are
// delivered again (to this member or to whoever gets the partition).
//
// A GroupConsumer is not safe for concurrent use; call Poll/Commit from
// one goroutine.
type GroupConsumer struct {
	c   *Client
	cfg GroupConfig

	mu         sync.Mutex
	coordAddr  string
	memberID   string
	generation int32
	assignment []protocol.TopicPartitions
	positions  map[fetchKey]int64

	needRejoin atomic.Bool
	hbStop     chan struct{}
	hbDone     chan struct{}
	closed     bool
}

type fetchKey struct {
	topic     string
	partition int32
}

// ErrGroupClosed is returned after Close.
var ErrGroupClosed = errors.New("client: group consumer closed")

// NewGroupConsumer creates a consumer; it joins the group on first Poll.
func (c *Client) NewGroupConsumer(cfg GroupConfig) (*GroupConsumer, error) {
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("client: group and topics are required")
	}
	if cfg.Strategy == "" {
		cfg.Strategy = "range"
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if cfg.RebalanceTimeout == 0 {
		cfg.RebalanceTimeout = 30 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = cfg.SessionTimeout / 3
	}
	cfg.Fetch.setDefaults()
	gc := &GroupConsumer{c: c, cfg: cfg, generation: -1, positions: map[fetchKey]int64{}}
	gc.needRejoin.Store(true)
	return gc, nil
}

// MemberID returns our member ID ("" before the first join).
func (gc *GroupConsumer) MemberID() string {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.memberID
}

// Generation returns the current group generation.
func (gc *GroupConsumer) Generation() int32 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.generation
}

// Assignment returns the partitions currently assigned to this member.
func (gc *GroupConsumer) Assignment() []protocol.TopicPartitions {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return append([]protocol.TopicPartitions(nil), gc.assignment...)
}

// Poll joins/rejoins the group if needed and returns the next records from
// the assigned partitions (possibly none, after the fetch wait).
func (gc *GroupConsumer) Poll(ctx context.Context) ([]Record, error) {
	if gc.isClosed() {
		return nil, ErrGroupClosed
	}
	if gc.needRejoin.Load() {
		if err := gc.rejoin(ctx); err != nil {
			return nil, err
		}
	}
	gc.mu.Lock()
	targets := make([]fetchTarget, 0, len(gc.positions))
	for k, off := range gc.positions {
		targets = append(targets, fetchTarget{k.topic, k.partition, off})
	}
	gc.mu.Unlock()
	if len(targets) == 0 {
		// Member with no partitions (more members than partitions).
		return nil, sleepCtx(ctx, gc.cfg.Fetch.MaxWait)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].topic != targets[j].topic {
			return targets[i].topic < targets[j].topic
		}
		return targets[i].partition < targets[j].partition
	})
	results, err := gc.c.fetchPartitions(ctx, gc.cfg.Fetch, targets)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		gc.c.RefreshMetadata(ctx)
		return nil, nil // transient; next Poll retries
	}
	var out []Record
	refresh := false
	for _, r := range results {
		key := fetchKey{r.target.topic, r.target.partition}
		switch {
		case r.Err == protocol.ErrNone:
			if len(r.Records) == 0 {
				continue
			}
			gc.mu.Lock()
			// Ignore results for partitions revoked by a concurrent rejoin.
			if cur, ok := gc.positions[key]; ok && cur == r.target.offset {
				gc.positions[key] = r.Records[len(r.Records)-1].Offset + 1
				out = append(out, r.Records...)
			}
			gc.mu.Unlock()
		case r.Err == protocol.ErrOffsetOutOfRange:
			off, err := gc.resetOffset(ctx, key)
			if err == nil {
				gc.mu.Lock()
				gc.positions[key] = off
				gc.mu.Unlock()
			}
		case r.Err.Retriable():
			refresh = true
		}
	}
	if refresh {
		gc.c.RefreshMetadata(ctx)
	}
	return out, nil
}

func (gc *GroupConsumer) resetOffset(ctx context.Context, k fetchKey) (int64, error) {
	which := protocol.OffsetEarliest
	if gc.cfg.Fetch.Reset == ResetLatest {
		which = protocol.OffsetLatest
	}
	return gc.c.ListOffsets(ctx, k.topic, k.partition, which)
}

// Seek sets the next offset to read for an assigned partition (e.g. to
// avoid committing records that were fetched but not processed).
func (gc *GroupConsumer) Seek(topic string, partition int32, offset int64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	k := fetchKey{topic, partition}
	if _, ok := gc.positions[k]; ok {
		gc.positions[k] = offset
	}
}

// Commit commits the position of every assigned partition (the offset of
// the next record to read). Call it after processing what Poll returned.
func (gc *GroupConsumer) Commit(ctx context.Context) error {
	gc.mu.Lock()
	addr, member, gen := gc.coordAddr, gc.memberID, gc.generation
	req := &protocol.OffsetCommitRequest{Group: gc.cfg.Group, MemberID: member, Generation: gen}
	for k, off := range gc.positions {
		req.Offsets = append(req.Offsets, protocol.CommitOffset{Topic: k.topic, Partition: k.partition, Offset: off})
	}
	gc.mu.Unlock()
	if member == "" || gen < 0 {
		return &protocol.Error{Code: protocol.ErrUnknownMemberID, Msg: "not a group member yet"}
	}
	if len(req.Offsets) == 0 {
		return nil
	}
	ctx, cancel := withDefaultTimeout(ctx, 15*time.Second)
	defer cancel()
	for attempt := 0; ; attempt++ {
		var resp protocol.SimpleResponse
		err := gc.c.call(ctx, addr, protocol.APIOffsetCommit, req, &resp)
		if err == nil {
			err = resp.Err.AsError(resp.ErrMsg)
		}
		switch codeOf(err) {
		case protocol.ErrNone:
			return nil
		case protocol.ErrIllegalGeneration, protocol.ErrUnknownMemberID, protocol.ErrRebalanceInProgress:
			// We are no longer the owner (or soon will not be): the commit
			// is rejected and the records will be redelivered.
			gc.needRejoin.Store(true)
			return err
		case protocol.ErrNotCoordinator, protocol.ErrNetwork:
			gc.needRejoin.Store(true)
			return err
		}
		if !isRetriable(err) || attempt >= 5 {
			return err
		}
		if sleepCtx(ctx, backoff(attempt)) != nil {
			return err
		}
	}
}

// Close commits positions, leaves the group (so partitions are reassigned
// immediately instead of after the session timeout) and stops heartbeats.
func (gc *GroupConsumer) Close(ctx context.Context) error {
	gc.mu.Lock()
	if gc.closed {
		gc.mu.Unlock()
		return nil
	}
	gc.closed = true
	gc.mu.Unlock()
	gc.stopHeartbeat()
	var err error
	if !gc.needRejoin.Load() {
		err = gc.Commit(ctx)
	}
	gc.mu.Lock()
	addr, member := gc.coordAddr, gc.memberID
	gc.mu.Unlock()
	if member != "" && addr != "" {
		var resp protocol.SimpleResponse
		lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		gc.c.call(lctx, addr, protocol.APILeaveGroup, &protocol.LeaveGroupRequest{Group: gc.cfg.Group, MemberID: member}, &resp)
		cancel()
	}
	return err
}

func (gc *GroupConsumer) isClosed() bool {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.closed
}

// ---------------------------------------------------------------- membership

func (gc *GroupConsumer) findCoordinator(ctx context.Context) (string, error) {
	var addr string
	err := gc.c.retry(ctx, func() error {
		var lastErr error
		for _, a := range gc.c.knownAddrs() {
			var resp protocol.FindCoordinatorResponse
			if err := gc.c.call(ctx, a, protocol.APIFindCoordinator, &protocol.FindCoordinatorRequest{Group: gc.cfg.Group}, &resp); err != nil {
				lastErr = err
				continue
			}
			if resp.Err != protocol.ErrNone {
				lastErr = resp.Err.AsError("find coordinator")
				continue
			}
			addr = resp.Addr
			return nil
		}
		if lastErr == nil {
			lastErr = &protocol.Error{Code: protocol.ErrCoordinatorNotAvailable}
		}
		return lastErr
	})
	return addr, err
}

// rejoin commits what we processed (if we still own it), then joins the
// group and loads committed offsets for the new assignment.
func (gc *GroupConsumer) rejoin(ctx context.Context) error {
	gc.stopHeartbeat()
	gc.mu.Lock()
	hadGeneration := gc.generation >= 0 && gc.memberID != ""
	gc.mu.Unlock()
	if hadGeneration {
		gc.Commit(ctx) // best effort: rejected if we were already kicked out
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		addr, err := gc.findCoordinator(ctx)
		if err != nil {
			return err
		}
		gc.mu.Lock()
		gc.coordAddr = addr
		req := &protocol.JoinGroupRequest{
			Group: gc.cfg.Group, MemberID: gc.memberID, ClientID: gc.c.cfg.ClientID,
			Topics: gc.cfg.Topics, Strategy: gc.cfg.Strategy,
			SessionTimeoutMs:   int32(gc.cfg.SessionTimeout / time.Millisecond),
			RebalanceTimeoutMs: int32(gc.cfg.RebalanceTimeout / time.Millisecond),
		}
		gc.mu.Unlock()

		jctx, cancel := context.WithTimeout(ctx, gc.cfg.RebalanceTimeout+10*time.Second)
		var resp protocol.JoinGroupResponse
		err = gc.c.call(jctx, addr, protocol.APIJoinGroup, req, &resp)
		cancel()
		if err == nil {
			err = resp.Err.AsError("join group")
		}
		switch codeOf(err) {
		case protocol.ErrNone:
			return gc.onJoined(ctx, addr, &resp)
		case protocol.ErrUnknownMemberID:
			gc.mu.Lock()
			gc.memberID = "" // the coordinator forgot us; join as new
			gc.mu.Unlock()
		case protocol.ErrRebalanceInProgress, protocol.ErrNotCoordinator, protocol.ErrCoordinatorNotAvailable, protocol.ErrNetwork:
		default:
			return err
		}
		if err := sleepCtx(ctx, backoff(attempt)); err != nil {
			return err
		}
	}
}

func (gc *GroupConsumer) onJoined(ctx context.Context, addr string, resp *protocol.JoinGroupResponse) error {
	// Load committed offsets for the new assignment.
	positions := map[fetchKey]int64{}
	if len(resp.Assignment) > 0 {
		var off protocol.OffsetFetchResponse
		err := gc.c.call(ctx, addr, protocol.APIOffsetFetch, &protocol.OffsetFetchRequest{Group: gc.cfg.Group, Partitions: resp.Assignment}, &off)
		if err == nil {
			err = off.Err.AsError("offset fetch")
		}
		if err != nil {
			gc.needRejoin.Store(true)
			return nil // retry the whole join on the next Poll
		}
		for _, o := range off.Offsets {
			k := fetchKey{o.Topic, o.Partition}
			if o.Offset >= 0 {
				positions[k] = o.Offset
				continue
			}
			start, err := gc.resetOffset(ctx, k)
			if err != nil {
				gc.needRejoin.Store(true)
				return nil
			}
			positions[k] = start
		}
	}
	gc.mu.Lock()
	gc.memberID = resp.MemberID
	gc.generation = resp.Generation
	gc.assignment = resp.Assignment
	gc.positions = positions
	gc.mu.Unlock()
	gc.needRejoin.Store(false)
	gc.c.log.Info("joined group", "group", gc.cfg.Group, "member", resp.MemberID,
		"generation", resp.Generation, "assignment", resp.Assignment)
	gc.startHeartbeat(addr, resp.MemberID, resp.Generation)
	return nil
}

func (gc *GroupConsumer) startHeartbeat(addr, member string, gen int32) {
	stop, done := make(chan struct{}), make(chan struct{})
	gc.mu.Lock()
	gc.hbStop, gc.hbDone = stop, done
	gc.mu.Unlock()
	go func() {
		defer close(done)
		t := time.NewTicker(gc.cfg.HeartbeatInterval)
		defer t.Stop()
		failures := 0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), gc.cfg.HeartbeatInterval*2)
			var resp protocol.SimpleResponse
			err := gc.c.call(ctx, addr, protocol.APIHeartbeat, &protocol.HeartbeatRequest{Group: gc.cfg.Group, MemberID: member, Generation: gen}, &resp)
			cancel()
			if err == nil && resp.Err == protocol.ErrNone {
				failures = 0
				continue
			}
			if err != nil {
				// Network trouble: tolerate a couple of misses, then assume
				// the coordinator moved and rejoin.
				failures++
				if failures < 3 {
					continue
				}
			}
			gc.needRejoin.Store(true)
			return
		}
	}()
}

func (gc *GroupConsumer) stopHeartbeat() {
	gc.mu.Lock()
	stop, done := gc.hbStop, gc.hbDone
	gc.hbStop, gc.hbDone = nil, nil
	gc.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}

// ---------------------------------------------------------------- admin

// GroupInfo describes a consumer group.
type GroupInfo struct {
	Group      string
	State      string
	Strategy   string
	Generation int32
	Members    []protocol.GroupMemberInfo
	Offsets    []GroupOffset
}

// GroupOffset is a committed offset with lag.
type GroupOffset struct {
	Topic     string
	Partition int32
	Committed int64
	End       int64 // high-watermark
	Lag       int64
}

// ListGroups asks every broker for the groups it coordinates.
func (c *Client) ListGroups(ctx context.Context) ([]protocol.GroupListing, error) {
	if err := c.RefreshMetadata(ctx); err != nil {
		return nil, err
	}
	seen := map[string]protocol.GroupListing{}
	for _, b := range c.Brokers() {
		var resp protocol.ListGroupsResponse
		if err := c.call(ctx, b.Addr, protocol.APIListGroups, &protocol.ListGroupsRequest{}, &resp); err != nil {
			continue
		}
		for _, g := range resp.Groups {
			seen[g.Group] = g
		}
	}
	out := make([]protocol.GroupListing, 0, len(seen))
	for _, g := range seen {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out, nil
}

// DescribeGroup returns members, assignment and committed offsets with lag.
func (c *Client) DescribeGroup(ctx context.Context, group string) (GroupInfo, error) {
	ctx, cancel := withDefaultTimeout(ctx, 15*time.Second)
	defer cancel()
	gc := &GroupConsumer{c: c, cfg: GroupConfig{Group: group}}
	info := GroupInfo{Group: group}
	err := c.retry(ctx, func() error {
		addr, err := gc.findCoordinator(ctx)
		if err != nil {
			return err
		}
		var d protocol.DescribeGroupResponse
		if err := c.call(ctx, addr, protocol.APIDescribeGroup, &protocol.DescribeGroupRequest{Group: group}, &d); err != nil {
			return err
		}
		if d.Err != protocol.ErrNone {
			return d.Err.AsError("describe group")
		}
		info.State, info.Strategy, info.Generation, info.Members = d.State, d.Strategy, d.Generation, d.Members
		var o protocol.OffsetFetchResponse
		if err := c.call(ctx, addr, protocol.APIOffsetFetch, &protocol.OffsetFetchRequest{Group: group}, &o); err != nil {
			return err
		}
		if o.Err != protocol.ErrNone {
			return o.Err.AsError("offset fetch")
		}
		info.Offsets = nil
		for _, off := range o.Offsets {
			g := GroupOffset{Topic: off.Topic, Partition: off.Partition, Committed: off.Offset, End: -1, Lag: -1}
			if end, err := c.ListOffsets(ctx, off.Topic, off.Partition, protocol.OffsetLatest); err == nil {
				g.End, g.Lag = end, max(0, end-off.Offset)
			}
			info.Offsets = append(info.Offsets, g)
		}
		return nil
	})
	return info, err
}

// CommitOffsets commits offsets outside of group membership ("simple"
// commit). The coordinator rejects it while the group has live members.
func (c *Client) CommitOffsets(ctx context.Context, group string, offsets []protocol.CommitOffset) error {
	ctx, cancel := withDefaultTimeout(ctx, 15*time.Second)
	defer cancel()
	gc := &GroupConsumer{c: c, cfg: GroupConfig{Group: group}}
	return c.retry(ctx, func() error {
		addr, err := gc.findCoordinator(ctx)
		if err != nil {
			return err
		}
		var resp protocol.SimpleResponse
		req := &protocol.OffsetCommitRequest{Group: group, Generation: -1, Offsets: offsets}
		if err := c.call(ctx, addr, protocol.APIOffsetCommit, req, &resp); err != nil {
			return err
		}
		return resp.Err.AsError(resp.ErrMsg)
	})
}

// CoordinatorInfo identifies a group coordinator.
type CoordinatorInfo struct {
	NodeID int32
	Addr   string
}

// FindCoordinator returns the broker currently coordinating group.
func (c *Client) FindCoordinator(ctx context.Context, group string) (CoordinatorInfo, error) {
	var info CoordinatorInfo
	err := c.retry(ctx, func() error {
		for _, a := range c.knownAddrs() {
			var resp protocol.FindCoordinatorResponse
			if err := c.call(ctx, a, protocol.APIFindCoordinator, &protocol.FindCoordinatorRequest{Group: group}, &resp); err != nil {
				continue
			}
			if resp.Err != protocol.ErrNone {
				return resp.Err.AsError("find coordinator")
			}
			info = CoordinatorInfo{NodeID: resp.NodeID, Addr: resp.Addr}
			return nil
		}
		return &protocol.Error{Code: protocol.ErrNetwork, Msg: "no broker reachable"}
	})
	return info, err
}

// RawCommit sends an OffsetCommit as-is to a coordinator (tools/tests).
func (c *Client) RawCommit(ctx context.Context, coord CoordinatorInfo, req *protocol.OffsetCommitRequest) error {
	var resp protocol.SimpleResponse
	if err := c.call(ctx, coord.Addr, protocol.APIOffsetCommit, req, &resp); err != nil {
		return err
	}
	return resp.Err.AsError(resp.ErrMsg)
}
