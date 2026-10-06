package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

const (
	historyLen     = 180 // samples kept for charts (3 minutes at 1 s)
	eventBufferLen = 300
	offsetsTopic   = "__consumer_offsets"
	dlqSuffix      = ".dlq"
)

// counterSample remembers a broker's counters to turn them into rates.
type counterSample struct {
	produced, consumed int64
	at                 time.Time
}

// collector builds snapshots.
type collector struct {
	g *Gateway

	lastBrokers map[int32]string // id -> protocol addr seen in metadata
	counters    map[int32]counterSample
	prevTotals  struct {
		produced, consumed int64
		at                 time.Time
		valid              bool
	}
	history []HistoryPoint
}

func newCollector(g *Gateway) *collector {
	return &collector{g: g, lastBrokers: map[int32]string{}, counters: map[int32]counterSample{}}
}

// metadata fetches cluster metadata, preferring the controller's view
// (follower metadata can lag the controller by a few milliseconds).
func (c *collector) metadata(ctx context.Context, prevController int32) (client.ClusterInfo, error) {
	var addrs []string
	if a, ok := c.lastBrokers[prevController]; ok {
		addrs = append(addrs, a)
	}
	for _, a := range c.lastBrokers {
		addrs = append(addrs, a)
	}
	addrs = append(addrs, c.g.bootstrap()...)
	var lastErr error
	seen := map[string]bool{}
	for _, a := range addrs {
		if seen[a] {
			continue
		}
		seen[a] = true
		cctx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
		info, err := c.g.client.DescribeClusterFrom(cctx, a)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		// If this broker knows a different controller, ask the controller.
		if info.ControllerID >= 0 && info.ControllerID != prevController {
			for _, b := range info.Brokers {
				if b.ID == info.ControllerID && b.Addr != a {
					cctx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
					if ci, err := c.g.client.DescribeClusterFrom(cctx, b.Addr); err == nil {
						info = ci
					}
					cancel()
				}
			}
		}
		for _, b := range info.Brokers {
			c.lastBrokers[b.ID] = b.Addr
		}
		return info, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no broker addresses known")
	}
	return client.ClusterInfo{}, lastErr
}

// fetchStates GETs /v1/state from every broker in parallel.
func (c *collector) fetchStates(ctx context.Context, httpAddrs map[int32]string) (map[int32]*broker.BrokerState, map[int32]string) {
	var mu sync.Mutex
	states := map[int32]*broker.BrokerState{}
	errs := map[int32]string{}
	var wg sync.WaitGroup
	for id, addr := range httpAddrs {
		if addr == "" {
			errs[id] = "no HTTP address advertised"
			continue
		}
		wg.Add(1)
		go func(id int32, addr string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(cctx, http.MethodGet, "http://"+addr+"/v1/state", nil)
			resp, err := c.g.httpc.Do(req)
			if err != nil {
				mu.Lock()
				errs[id] = "unreachable"
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
			var st broker.BrokerState
			if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
				mu.Lock()
				errs[id] = "bad /v1/state response"
				mu.Unlock()
				return
			}
			mu.Lock()
			states[id] = &st
			mu.Unlock()
		}(id, addr)
	}
	wg.Wait()
	return states, errs
}

func tpKey(topic string, p int32) string { return topic + "-" + strconv.Itoa(int(p)) }

func isInternal(topic string) bool { return strings.HasPrefix(topic, "__") }
func isDLQ(topic string) bool      { return strings.HasSuffix(topic, dlqSuffix) }

// collect builds one snapshot.
func (c *collector) collect(ctx context.Context, prev *Snapshot) *Snapshot {
	now := time.Now()
	snap := &Snapshot{
		Time: now.UnixMilli(), Mode: c.g.mode(), ControllerID: -1,
		Brokers: []BrokerView{}, Topics: []TopicView{}, Groups: []GroupView{},
		Capabilities: c.g.capabilities(),
	}
	prevController := int32(-1)
	if prev != nil {
		prevController = prev.ControllerID
	}
	info, err := c.metadata(ctx, prevController)
	if err != nil {
		snap.Error = "cluster unreachable: " + err.Error()
		snap.Brokers = c.g.offlineBrokerViews(prev)
		snap.Totals.Brokers = len(snap.Brokers)
		snap.Totals.OfflineBrokers = len(snap.Brokers)
		snap.History = c.appendHistory(HistoryPoint{T: snap.Time, BrokerIn: map[string]float64{}})
		snap.Demo = c.g.demo.view()
		snap.DLQIndex = c.g.demo.dlqIndex()
		return snap
	}
	snap.Reachable = true
	snap.ControllerID = info.ControllerID

	// Broker HTTP addresses: from metadata, with the managed launcher as a
	// fallback for brokers that have not registered yet.
	httpAddrs := map[int32]string{}
	for _, b := range info.Brokers {
		httpAddrs[b.ID] = b.HTTPAddr
	}
	for id, addr := range c.g.managedHTTPAddrs() {
		if httpAddrs[id] == "" {
			httpAddrs[id] = addr
		}
	}
	states, stateErrs := c.fetchStates(ctx, httpAddrs)

	// Partition state as reported by each partition's leader.
	type pstate struct{ logStart, logEnd, hw, size int64 }
	leaderState := map[string]pstate{}
	anyState := map[string]pstate{}
	for id, st := range states {
		for _, p := range st.Partitions {
			ps := pstate{p.LogStart, p.LogEnd, p.HighWater, p.SizeBytes}
			k := tpKey(p.Topic, p.Partition)
			if p.Role == "leader" && p.Leader == id {
				leaderState[k] = ps
			}
			if cur, ok := anyState[k]; !ok || ps.logEnd > cur.logEnd {
				anyState[k] = ps
			}
		}
	}

	// Brokers.
	ids := make([]int32, 0, len(info.Brokers))
	meta := map[int32]protocol.BrokerInfo{}
	for _, b := range info.Brokers {
		ids = append(ids, b.ID)
		meta[b.ID] = b
	}
	for id := range c.g.managedHTTPAddrs() {
		if _, ok := meta[id]; !ok {
			ids = append(ids, id)
			meta[id] = protocol.BrokerInfo{ID: id, Addr: c.g.managedAddr(id), HTTPAddr: httpAddrs[id]}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	views := map[int32]*BrokerView{}
	for _, id := range ids {
		m := meta[id]
		bv := BrokerView{ID: id, Addr: m.Addr, HTTPAddr: httpAddrs[id], Fenced: m.Fenced,
			IsController: id == info.ControllerID, LeaderPartitions: []string{}, ReplicaPartitions: []string{}}
		if st, ok := states[id]; ok {
			bv.Reachable = true
			bv.StorageBytes = st.StorageBytes
			bv.UptimeSeconds = st.UptimeSeconds
			bv.Counters = st.Counters
			if st.Raft != nil {
				bv.RaftRole, bv.RaftTerm = st.Raft.Role, st.Raft.Term
			}
			prod, cons := st.Counters["records_produced"], st.Counters["records_consumed"]
			if prevC, ok := c.counters[id]; ok {
				dt := now.Sub(prevC.at).Seconds()
				if dt > 0 {
					// A counter that went down means the broker restarted.
					bv.InMsgPerSec = rate(prod, prevC.produced, dt)
					bv.OutMsgPerSec = rate(cons, prevC.consumed, dt)
				}
			}
			c.counters[id] = counterSample{prod, cons, now}
			snap.Totals.ProduceErrors += st.Counters["produce_errors"]
		} else {
			bv.StatusDetail = stateErrs[id]
			delete(c.counters, id)
		}
		if pv := c.g.processView(id); pv != nil {
			bv.Process = pv
			if !bv.Reachable && pv.State != "running" && pv.State != "starting" {
				bv.StatusDetail = "process " + pv.State
			}
		}
		switch {
		case !bv.Reachable:
			bv.Status = "offline"
		case bv.Fenced:
			bv.Status = "fenced"
		default:
			bv.Status = "healthy"
		}
		views[id] = &bv
	}

	// Topics and partitions.
	hw := map[string]int64{}
	for _, t := range info.Topics {
		tv := TopicView{Name: t.Name, Internal: isInternal(t.Name), DLQ: isDLQ(t.Name), Configs: t.Configs, Partitions: []PartitionView{}}
		for _, p := range t.Partitions {
			pv := PartitionView{ID: p.ID, Leader: p.Leader, LeaderEpoch: p.LeaderEpoch,
				Replicas: nonNil(p.Replicas), ISR: nonNil(p.ISR), HighWatermark: -1, LogEnd: -1, LogStart: -1}
			k := tpKey(t.Name, p.ID)
			if ls, ok := leaderState[k]; ok {
				pv.LogStart, pv.LogEnd, pv.HighWatermark, pv.SizeBytes = ls.logStart, ls.logEnd, ls.hw, ls.size
			} else if as, ok := anyState[k]; ok {
				pv.LogStart, pv.LogEnd, pv.SizeBytes = as.logStart, as.logEnd, as.size // leader unreachable: no HW
			}
			pv.Offline = p.Leader < 0 || (views[p.Leader] != nil && !views[p.Leader].Reachable)
			pv.UnderReplicated = len(p.ISR) < len(p.Replicas)
			if pv.HighWatermark >= 0 {
				hw[k] = pv.HighWatermark
			}
			if pv.LogEnd > 0 {
				tv.Messages += pv.LogEnd
			}
			for _, r := range p.Replicas {
				if bv := views[r]; bv != nil {
					if r == p.Leader {
						bv.LeaderPartitions = append(bv.LeaderPartitions, k)
					} else {
						bv.ReplicaPartitions = append(bv.ReplicaPartitions, k)
					}
				}
			}
			if !tv.Internal {
				snap.Totals.Partitions++
				if pv.Offline {
					snap.Totals.OfflinePartitions++
				}
				if pv.UnderReplicated {
					snap.Totals.UnderReplicated++
				}
			}
			tv.Partitions = append(tv.Partitions, pv)
		}
		switch {
		case tv.Internal:
		case tv.DLQ:
			snap.Totals.DLQMessages += tv.Messages
		default:
			snap.Totals.Topics++
			snap.Totals.MessagesProduced += tv.Messages
		}
		snap.Topics = append(snap.Topics, tv)
	}

	// Consumer groups (lag computed from the leaders' high-watermarks).
	var prevGroups []GroupView
	if prev != nil {
		prevGroups = prev.Groups
	}
	snap.Groups = c.collectGroups(ctx, hw, prevGroups)
	lagByPartition := map[string]map[string]int64{}
	for _, g := range snap.Groups {
		if len(g.Members) > 0 {
			snap.Totals.ActiveGroups++
		}
		snap.Totals.ConsumerLag += g.TotalLag
		for _, o := range g.Offsets {
			if o.Committed > 0 {
				snap.Totals.MessagesConsumed += o.Committed
			}
			k := tpKey(o.Topic, o.Partition)
			if lagByPartition[k] == nil {
				lagByPartition[k] = map[string]int64{}
			}
			lagByPartition[k][g.ID] = o.Lag
		}
	}
	for ti := range snap.Topics {
		for pi := range snap.Topics[ti].Partitions {
			p := &snap.Topics[ti].Partitions[pi]
			if gl := lagByPartition[tpKey(snap.Topics[ti].Name, p.ID)]; gl != nil {
				p.GroupLag = gl
				for _, l := range gl {
					p.Lag += l
				}
			}
		}
	}

	for _, id := range ids {
		bv := views[id]
		snap.Brokers = append(snap.Brokers, *bv)
		snap.Totals.Brokers++
		if bv.Status == "healthy" {
			snap.Totals.HealthyBrokers++
		}
		if bv.Status == "offline" {
			snap.Totals.OfflineBrokers++
		}
	}

	// Cluster-wide rates from durable totals (immune to broker restarts).
	if c.prevTotals.valid {
		dt := now.Sub(c.prevTotals.at).Seconds()
		if dt > 0 {
			snap.Totals.ProducedPerSec = rate(snap.Totals.MessagesProduced, c.prevTotals.produced, dt)
			snap.Totals.ConsumedPerSec = rate(snap.Totals.MessagesConsumed, c.prevTotals.consumed, dt)
		}
	}
	c.prevTotals.produced, c.prevTotals.consumed, c.prevTotals.at, c.prevTotals.valid =
		snap.Totals.MessagesProduced, snap.Totals.MessagesConsumed, now, true

	snap.Demo = c.g.demo.view()
	snap.Totals.ProcessingFailures = c.g.demo.failures()
	snap.DLQIndex = c.g.demo.dlqIndex()

	point := HistoryPoint{T: snap.Time, ProducedPerSec: snap.Totals.ProducedPerSec, ConsumedPerSec: snap.Totals.ConsumedPerSec,
		Lag: snap.Totals.ConsumerLag, BrokerIn: map[string]float64{}, HealthyBrokers: snap.Totals.HealthyBrokers}
	for _, b := range snap.Brokers {
		point.BrokerIn[strconv.Itoa(int(b.ID))] = b.InMsgPerSec
	}
	snap.History = c.appendHistory(point)
	return snap
}

func (c *collector) appendHistory(p HistoryPoint) []HistoryPoint {
	c.history = append(c.history, p)
	if len(c.history) > historyLen {
		c.history = c.history[len(c.history)-historyLen:]
	}
	return append([]HistoryPoint(nil), c.history...)
}

func rate(cur, prev int64, dt float64) float64 {
	if cur < prev { // counter reset (restart) or retention: no meaningful rate
		return 0
	}
	return float64(cur-prev) / dt
}

func nonNil(v []int32) []int32 {
	if v == nil {
		return []int32{}
	}
	return v
}

// collectGroups lists groups and describes each one.
// staleGroupTTL is how long a group that cannot be described (its
// coordinator is moving) keeps its last known view instead of vanishing.
const staleGroupTTL = 30 * time.Second

// collectGroups describes every group. While a group's coordinator is
// failing over, ListGroups may not report the group and DescribeGroup
// fails; dropping it would make "messages consumed" fall and then jump
// back (a fake throughput spike), so the previous view is carried forward
// for up to staleGroupTTL, flagged Stale.
func (c *collector) collectGroups(ctx context.Context, hw map[string]int64, prev []GroupView) []GroupView {
	now := time.Now().UnixMilli()
	prevByID := map[string]GroupView{}
	for _, g := range prev {
		prevByID[g.ID] = g
	}
	carry := func(id, why string) (GroupView, bool) {
		p, ok := prevByID[id]
		if !ok || len(p.Offsets) == 0 {
			return GroupView{}, false
		}
		since := p.StaleSince
		if since == 0 {
			since = now
		}
		if now-since > staleGroupTTL.Milliseconds() {
			return GroupView{}, false
		}
		p.Stale, p.StaleSince, p.Error = true, since, why
		return p, true
	}
	gctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	listing, err := c.g.client.ListGroups(gctx)
	if err != nil {
		listing = nil
	}
	listed := map[string]bool{}
	out := make([]GroupView, 0, len(listing))
	for _, l := range listing {
		listed[l.Group] = true
		dctx, dcancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		info, err := c.g.client.DescribeGroupOffsets(dctx, l.Group)
		dcancel()
		gv := GroupView{ID: l.Group, State: l.State, Members: []MemberView{}, Offsets: []GroupOffsetView{}}
		if err != nil {
			if p, ok := carry(l.Group, "coordinator unavailable: "+err.Error()); ok {
				out = append(out, p)
				continue
			}
			gv.Error = err.Error()
			out = append(out, gv)
			continue
		}
		gv.State, gv.Strategy, gv.Generation = info.State, info.Strategy, info.Generation
		owner := map[string]string{}
		for _, m := range info.Members {
			mv := MemberView{ID: m.MemberID, ClientID: m.ClientID, Partitions: []string{}}
			for _, tp := range m.Assignment {
				for _, p := range tp.Partitions {
					k := tpKey(tp.Topic, p)
					mv.Partitions = append(mv.Partitions, k)
					owner[k] = m.MemberID
				}
			}
			gv.Members = append(gv.Members, mv)
		}
		seen := map[string]bool{}
		for _, o := range info.Offsets {
			k := tpKey(o.Topic, o.Partition)
			seen[k] = true
			ov := GroupOffsetView{Topic: o.Topic, Partition: o.Partition, Committed: o.Committed, End: -1, Lag: 0, Owner: owner[k]}
			if h, ok := hw[k]; ok {
				ov.End = h
				ov.Lag = max(0, h-max(o.Committed, 0))
			}
			gv.TotalLag += ov.Lag
			gv.Offsets = append(gv.Offsets, ov)
		}
		// Assigned partitions with no commit yet: everything is lag.
		for k, m := range owner {
			if seen[k] {
				continue
			}
			i := strings.LastIndex(k, "-")
			p, _ := strconv.Atoi(k[i+1:])
			ov := GroupOffsetView{Topic: k[:i], Partition: int32(p), Committed: -1, End: -1, Owner: m}
			if h, ok := hw[k]; ok {
				ov.End, ov.Lag = h, h
			}
			gv.TotalLag += ov.Lag
			gv.Offsets = append(gv.Offsets, ov)
		}
		sort.Slice(gv.Offsets, func(i, j int) bool {
			if gv.Offsets[i].Topic != gv.Offsets[j].Topic {
				return gv.Offsets[i].Topic < gv.Offsets[j].Topic
			}
			return gv.Offsets[i].Partition < gv.Offsets[j].Partition
		})
		out = append(out, gv)
	}
	for id := range prevByID {
		if !listed[id] {
			if p, ok := carry(id, "coordinator moving: group not listed by any broker"); ok {
				out = append(out, p)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
