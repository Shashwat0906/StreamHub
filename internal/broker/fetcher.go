package broker

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

// ---------------------------------------------------------------- follower side of Partition

// appendReplicated appends records fetched from the leader. It is a no-op
// (returns false) if we are no longer a follower at epoch.
func (p *Partition) appendReplicated(epoch int32, recs []storage.Record, leaderHW int64) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != roleFollower || p.leaderEpoch != epoch {
		return false, nil
	}
	if len(recs) > 0 {
		if err := p.log.AppendReplicated(recs); err != nil {
			return false, err
		}
		for i := range recs {
			observeRecord(p.producers, recs[i].ProducerID, recs[i].Sequence, recs[i].Offset)
		}
	}
	// Follower HW = min(leader HW, our LEO): we can only vouch for what we
	// actually have.
	hw := min(leaderHW, p.log.EndOffset())
	if hw > p.hw {
		p.hw = hw
	}
	go p.wakeWaiters()
	return true, nil
}

// truncateForLeader removes a divergent tail using the leader's answer to
// OffsetForLeaderEpoch. leaderEpoch/leaderEnd come from the leader; -1
// means the leader has no epoch <= ours, in which case we fall back to
// our high-watermark (everything below it is known to be committed).
func (p *Partition) truncateForLeader(epoch int32, leaderEpoch int32, leaderEnd int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != roleFollower || p.leaderEpoch != epoch {
		return nil
	}
	leo := p.log.EndOffset()
	target := leo
	if leaderEpoch < 0 || leaderEnd < 0 {
		target = min(leo, p.hw)
	} else {
		// If the leader answered for an older epoch than we asked about,
		// our log may contain records from epochs the leader never saw.
		// Cut at the end of that older epoch *in our own log* too (KIP-279).
		_, localEnd := p.log.Epochs().EndOffsetFor(leaderEpoch, leo)
		target = min(leo, leaderEnd)
		if localEnd >= 0 {
			target = min(target, localEnd)
		}
	}
	if target >= leo {
		return nil
	}
	if err := p.log.TruncateTo(target); err != nil {
		return err
	}
	if p.hw > target {
		p.hw = target
	}
	p.rebuildProducersLocked()
	return nil
}

// ---------------------------------------------------------------- leader side: ISR maintenance

// isrChange computes the ISR this leader wants, given follower progress.
// It returns nil if no change is needed or one is already in flight.
func (p *Partition) isrChange(now time.Time, lag time.Duration, live func(int32) bool) (newISR []int32, epoch int32, shrink bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != roleLeader || p.pendingISR != nil {
		return nil, 0, false
	}
	want := []int32{p.self}
	for _, r := range p.replicas {
		if r == p.self {
			continue
		}
		f := p.followers[r]
		inISR := slices.Contains(p.isr, r)
		switch {
		case inISR && f != nil && now.Sub(f.lastCaughtUpTime) <= lag:
			want = append(want, r) // still in sync
		case !inISR && f != nil && f.leo >= p.hw && f.leo >= p.epochStartLocked() && live(r):
			// Caught up to the HW (and into the current leader epoch, so it
			// has truncated any divergent tail): eligible to rejoin.
			want = append(want, r)
		}
	}
	if slices.Equal(sortedCopy(want), sortedCopy(p.isr)) {
		return nil, 0, false
	}
	shrink = len(want) < len(p.isr)
	p.pendingISR = want
	return want, p.leaderEpoch, shrink
}

// epochStartLocked is the first offset written in the current leader
// epoch (or LEO if nothing was written yet).
func (p *Partition) epochStartLocked() int64 {
	for _, e := range slices.Backward(p.log.Epochs().Entries()) {
		if e.Epoch == p.leaderEpoch {
			return e.StartOffset
		}
		if e.Epoch < p.leaderEpoch {
			break
		}
	}
	return p.log.EndOffset()
}

func (p *Partition) clearPendingISR() {
	p.mu.Lock()
	p.pendingISR = nil
	p.mu.Unlock()
}

// checkISR runs on every broker; it only acts on partitions we lead.
func (rm *ReplicaManager) checkISR() {
	b := rm.b
	if b.raft == nil || b.isFenced() {
		return
	}
	now := time.Now()
	live := func(id int32) bool {
		bm, ok := b.meta.Broker(id)
		return ok && !bm.Fenced
	}
	for _, p := range rm.all() {
		newISR, epoch, shrink := p.isrChange(now, b.cfg.ReplicaLagTime, live)
		if newISR == nil {
			continue
		}
		req := &protocol.AlterISRRequest{BrokerID: b.cfg.ID, Topic: p.Topic, Partition: p.ID, LeaderEpoch: epoch, NewISR: newISR}
		go rm.sendAlterISR(p, req, shrink)
	}
}

func (rm *ReplicaManager) sendAlterISR(p *Partition, req *protocol.AlterISRRequest, shrink bool) {
	b := rm.b
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()
	var resp *protocol.SimpleResponse
	leader := b.proposer.LeaderID()
	if leader == b.cfg.ID {
		resp = b.controller.onAlterISR(ctx, req)
	} else if addr, ok := b.brokerAddr(leader); ok {
		var r protocol.SimpleResponse
		if err := b.pool.Call(ctx, addr, protocol.APIAlterISR, req, &r); err == nil {
			resp = &r
		}
	}
	if resp == nil || resp.Err != protocol.ErrNone {
		// Retry on a later tick (the metadata may also have moved on).
		p.clearPendingISR()
		if resp != nil {
			b.logger.Info("AlterISR rejected", "partition", p.String(), "isr", req.NewISR, "err", resp.Err.String(), "msg", resp.ErrMsg)
		}
		return
	}
	if shrink {
		b.metrics.isrShrinks.Inc()
	} else {
		b.metrics.isrExpands.Inc()
	}
	b.logger.Info("ISR changed", "partition", p.String(), "isr", req.NewISR, "shrink", shrink)
	// The new ISR becomes effective when the committed metadata is applied
	// (reconcile clears pendingISR then).
}

// ---------------------------------------------------------------- fetchers

// fetcherManager runs one fetcher goroutine per leader broker. Each
// fetcher replicates every partition this broker follows from that leader
// using multi-partition long-poll Fetch requests (like Kafka's
// ReplicaFetcherThread).
type fetcherManager struct {
	rm       *ReplicaManager
	mu       sync.Mutex
	fetchers map[int32]*fetcher
	owner    map[*Partition]int32
	closed   bool
}

type fetchState struct {
	p           *Partition
	epoch       int32
	needsTrunc  bool
	backoffTill time.Time
}

type fetcher struct {
	fm     *fetcherManager
	leader int32
	mu     sync.Mutex
	parts  map[*Partition]*fetchState
	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}
}

func newFetcherManager(rm *ReplicaManager) *fetcherManager {
	return &fetcherManager{rm: rm, fetchers: map[int32]*fetcher{}, owner: map[*Partition]int32{}}
}

// add starts replicating p from leader at epoch (replacing any previous
// assignment). Every (re)assignment begins with leader-epoch truncation.
func (fm *fetcherManager) add(p *Partition, leader int32, epoch int32) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.closed {
		return
	}
	fm.removeLocked(p)
	f := fm.fetchers[leader]
	if f == nil {
		f = &fetcher{fm: fm, leader: leader, parts: map[*Partition]*fetchState{},
			wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
		fm.fetchers[leader] = f
		go f.run()
	}
	f.mu.Lock()
	f.parts[p] = &fetchState{p: p, epoch: epoch, needsTrunc: true}
	f.mu.Unlock()
	fm.owner[p] = leader
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (fm *fetcherManager) remove(p *Partition) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.removeLocked(p)
}

func (fm *fetcherManager) removeLocked(p *Partition) {
	leader, ok := fm.owner[p]
	if !ok {
		return
	}
	delete(fm.owner, p)
	f := fm.fetchers[leader]
	f.mu.Lock()
	delete(f.parts, p)
	empty := len(f.parts) == 0
	f.mu.Unlock()
	if empty {
		delete(fm.fetchers, leader)
		close(f.stop) // the goroutine exits on its own; do not wait under lock
	}
}

func (fm *fetcherManager) closeAll() {
	fm.mu.Lock()
	fm.closed = true
	all := fm.fetchers
	fm.fetchers = map[int32]*fetcher{}
	fm.owner = map[*Partition]int32{}
	fm.mu.Unlock()
	for _, f := range all {
		close(f.stop)
		<-f.done
	}
}

func (f *fetcher) stopped() bool {
	select {
	case <-f.stop:
		return true
	default:
		return false
	}
}

func (f *fetcher) sleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-f.stop:
	case <-f.wake:
	}
}

func (f *fetcher) run() {
	defer close(f.done)
	b := f.fm.rm.b
	log := b.logger.With("fetcher_leader", f.leader)
	for !f.stopped() {
		addr, ok := b.brokerAddr(f.leader)
		if !ok {
			f.sleep(200 * time.Millisecond)
			continue
		}
		f.mu.Lock()
		states := make([]*fetchState, 0, len(f.parts))
		for _, s := range f.parts {
			states = append(states, s)
		}
		f.mu.Unlock()
		if len(states) == 0 {
			f.sleep(time.Second)
			continue
		}

		// Step 1: truncate partitions that were just (re)assigned.
		now := time.Now()
		var ready []*fetchState
		for _, s := range states {
			if now.Before(s.backoffTill) {
				continue
			}
			if s.needsTrunc {
				if err := f.truncate(addr, s); err != nil {
					log.Debug("truncation step failed", "partition", s.p.String(), "err", err)
					s.backoffTill = time.Now().Add(200 * time.Millisecond)
					continue
				}
				s.needsTrunc = false
			}
			ready = append(ready, s)
		}
		if len(ready) == 0 {
			f.sleep(100 * time.Millisecond)
			continue
		}

		// Step 2: one long-poll fetch for all ready partitions.
		req := &protocol.FetchRequest{
			ReplicaID: b.cfg.ID,
			MaxWaitMs: int32(b.cfg.ReplicaFetchWait / time.Millisecond),
			MinBytes:  1,
		}
		for _, s := range ready {
			req.Partitions = append(req.Partitions, protocol.FetchPartition{
				Topic: s.p.Topic, Partition: s.p.ID, FetchOffset: s.p.log.EndOffset(),
				MaxBytes: 1 << 20, CurrentLeaderEpoch: s.epoch,
			})
		}
		ctx, cancel := context.WithTimeout(b.ctx, b.cfg.ReplicaFetchWait+5*time.Second)
		var resp protocol.FetchResponse
		err := b.pool.Call(ctx, addr, protocol.APIFetch, req, &resp)
		cancel()
		if err != nil {
			if !f.stopped() {
				log.Debug("replica fetch failed", "err", err)
				f.sleep(100 * time.Millisecond)
			}
			continue
		}
		for i, pr := range resp.Partitions {
			if i >= len(ready) {
				break
			}
			f.handle(ready[i], pr)
		}
	}
}

// truncate asks the leader where our latest epoch ends and cuts our log.
func (f *fetcher) truncate(addr string, s *fetchState) error {
	b := f.fm.rm.b
	latest := s.p.log.Epochs().LatestEpoch()
	if latest < 0 {
		return nil // empty log: nothing can diverge
	}
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()
	var resp protocol.OffsetForLeaderEpochResponse
	req := &protocol.OffsetForLeaderEpochRequest{Topic: s.p.Topic, Partition: s.p.ID, LeaderEpoch: latest}
	if err := b.pool.Call(ctx, addr, protocol.APIOffsetForLeaderEpoch, req, &resp); err != nil {
		return err
	}
	if resp.Err != protocol.ErrNone {
		return resp.Err.AsError("offset for leader epoch")
	}
	before := s.p.log.EndOffset()
	if err := s.p.truncateForLeader(s.epoch, resp.LeaderEpoch, resp.EndOffset); err != nil {
		return err
	}
	if after := s.p.log.EndOffset(); after < before {
		b.logger.Info("truncated divergent log tail", "partition", s.p.String(),
			"from", before, "to", after, "leader", f.leader, "leader_epoch", s.epoch)
	}
	return nil
}

func (f *fetcher) handle(s *fetchState, pr protocol.FetchPartitionResponse) {
	b := f.fm.rm.b
	switch pr.Err {
	case protocol.ErrNone:
		ok, err := s.p.appendReplicated(s.epoch, pr.Records, pr.HighWatermark)
		if errors.Is(err, storage.ErrNonSequential) {
			s.needsTrunc = true // our log changed under us; resync
		} else if err != nil {
			b.logger.Error("replicated append failed", "partition", s.p.String(), "err", err)
			s.backoffTill = time.Now().Add(time.Second)
		} else if !ok {
			s.backoffTill = time.Now().Add(100 * time.Millisecond)
		}
	case protocol.ErrOffsetOutOfRange:
		leo := s.p.log.EndOffset()
		switch {
		case leo < pr.LogStartOffset:
			// The leader deleted what we would need (retention): start over
			// from its log start.
			b.logger.Warn("follower behind leader log start, resetting", "partition", s.p.String(),
				"leo", leo, "leader_log_start", pr.LogStartOffset)
			s.p.log.ResetTo(pr.LogStartOffset)
		default:
			s.needsTrunc = true
		}
	default:
		// NOT_LEADER / FENCED / UNKNOWN epoch: metadata is changing. The
		// reconcile loop will reassign us; until then back off.
		s.backoffTill = time.Now().Add(200 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- OffsetForLeaderEpoch (leader)

func (b *Broker) handleOffsetForLeaderEpoch(req *protocol.OffsetForLeaderEpochRequest) protocol.Message {
	p, code := b.partitionOrError(req.Topic, req.Partition)
	if code != protocol.ErrNone {
		return &protocol.OffsetForLeaderEpochResponse{Err: code, LeaderEpoch: -1, EndOffset: -1}
	}
	r, _, epoch, _, _ := p.Snapshot()
	if r != roleLeader {
		return &protocol.OffsetForLeaderEpochResponse{Err: protocol.ErrNotLeader, LeaderEpoch: -1, EndOffset: -1}
	}
	if req.LeaderEpoch > epoch {
		return &protocol.OffsetForLeaderEpochResponse{Err: protocol.ErrUnknownLeaderEpoch, LeaderEpoch: -1, EndOffset: -1}
	}
	e, end := p.log.Epochs().EndOffsetFor(req.LeaderEpoch, p.log.EndOffset())
	return &protocol.OffsetForLeaderEpochResponse{LeaderEpoch: e, EndOffset: end}
}

var _ = metadata.NoLeader
