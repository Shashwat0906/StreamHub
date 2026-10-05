package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

type role int

const (
	roleNone role = iota
	roleLeader
	roleFollower
)

func (r role) String() string {
	switch r {
	case roleLeader:
		return "leader"
	case roleFollower:
		return "follower"
	}
	return "none"
}

// followerState is what a leader knows about one follower replica.
type followerState struct {
	leo              int64     // follower's log end offset (from its last fetch); -1 = unknown
	lastCaughtUpTime time.Time // last time the follower had everything the leader had
	lastFetchTime    time.Time
	// leader LEO at the time of the previous fetch; see updateFollowerLocked.
	lastFetchLeaderLEO int64
}

// Partition is this broker's replica of one partition.
type Partition struct {
	Topic string
	ID    int32
	self  int32
	log   *storage.Log

	mu             sync.Mutex
	role           role
	leaderID       int32
	leaderEpoch    int32
	partitionEpoch int32
	replicas       []int32
	isr            []int32 // committed ISR from metadata
	// pendingISR is an ISR change we asked the controller for and that is
	// not committed yet. While an expansion is pending the HW is computed
	// over the union ("maximal ISR"), which is the conservative choice.
	pendingISR []int32
	hw         int64
	followers  map[int32]*followerState
	producers  map[int64]*producerState

	waitMu  sync.Mutex
	waiters map[chan struct{}]struct{}
}

func newPartition(topic string, id int32, self int32, log *storage.Log, checkpointHW int64) *Partition {
	hw := checkpointHW
	if end := log.EndOffset(); hw > end {
		hw = end
	}
	if start := log.StartOffset(); hw < start {
		hw = start
	}
	p := &Partition{
		Topic: topic, ID: id, self: self, log: log,
		leaderID: metadata.NoLeader, hw: hw,
		followers: map[int32]*followerState{},
		waiters:   map[chan struct{}]struct{}{},
	}
	p.producers = rebuildProducerState(log)
	return p
}

func (p *Partition) String() string { return fmt.Sprintf("%s-%d", p.Topic, p.ID) }

// HighWatermark returns the current HW.
func (p *Partition) HighWatermark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hw
}

// Snapshot returns a view of the replica state for diagnostics.
func (p *Partition) Snapshot() (r role, leader int32, epoch int32, hw int64, isr []int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.role, p.leaderID, p.leaderEpoch, p.hw, slices.Clone(p.isr)
}

// ---------------------------------------------------------------- waiters

// addWaiter registers ch to be signalled when the log end or HW moves.
func (p *Partition) addWaiter(ch chan struct{}) {
	p.waitMu.Lock()
	p.waiters[ch] = struct{}{}
	p.waitMu.Unlock()
}

func (p *Partition) removeWaiter(ch chan struct{}) {
	p.waitMu.Lock()
	delete(p.waiters, ch)
	p.waitMu.Unlock()
}

func (p *Partition) wakeWaiters() {
	p.waitMu.Lock()
	for ch := range p.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	p.waitMu.Unlock()
}

// ---------------------------------------------------------------- role changes

// applyMetadata updates this replica from the committed metadata. It
// returns the new role and whether the leader or epoch changed (in which
// case the caller (re)starts/stops fetching).
func (p *Partition) applyMetadata(m metadata.PartitionMeta) (newRole role, changed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case m.Leader == p.self:
		newRole = roleLeader
	case slices.Contains(m.Replicas, p.self) && m.Leader != metadata.NoLeader:
		newRole = roleFollower
	default:
		newRole = roleNone
	}
	changed = newRole != p.role || m.LeaderEpoch != p.leaderEpoch || m.Leader != p.leaderID
	p.replicas = slices.Clone(m.Replicas)
	p.isr = slices.Clone(m.ISR)
	p.partitionEpoch = m.PartitionEpoch
	if p.pendingISR != nil && slices.Equal(sortedCopy(p.pendingISR), sortedCopy(m.ISR)) {
		p.pendingISR = nil
	}
	if changed {
		p.pendingISR = nil
		p.leaderID = m.Leader
		p.leaderEpoch = m.LeaderEpoch
		if newRole == roleLeader {
			// New leader: we do not yet know where followers are. Mark them
			// unknown; the HW cannot advance until every ISR follower has
			// fetched once (or has been removed from the ISR).
			now := time.Now()
			p.followers = map[int32]*followerState{}
			for _, r := range m.Replicas {
				if r != p.self {
					p.followers[r] = &followerState{leo: -1, lastCaughtUpTime: now, lastFetchTime: now}
				}
			}
		}
	}
	p.role = newRole
	if newRole == roleLeader {
		p.maybeAdvanceHWLocked()
	}
	return newRole, changed
}

func sortedCopy(v []int32) []int32 {
	c := slices.Clone(v)
	slices.Sort(c)
	return c
}

// ---------------------------------------------------------------- leader: produce

// appendAsLeader validates and appends a produce batch. It returns the
// base offset and the offset just after the batch (the HW the batch needs
// for acks=all).
func (p *Partition) appendAsLeader(req *protocol.ProduceRequest, minISR int) (base, end int64, epoch int32, code protocol.ErrorCode, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != roleLeader {
		return -1, -1, 0, protocol.ErrNotLeader, fmt.Sprintf("leader is %d", p.leaderID)
	}
	// acks=all needs enough in-sync replicas *before* we write, otherwise
	// we would append data we cannot make durable (Kafka: NOT_ENOUGH_REPLICAS).
	if req.Acks == protocol.AcksAll && len(p.isr) < minISR {
		return -1, -1, 0, protocol.ErrNotEnoughReplicas, fmt.Sprintf("isr %v < min.insync.replicas %d", p.isr, minISR)
	}

	// Idempotence: duplicate batches are acknowledged without re-appending.
	if req.ProducerID >= 0 {
		dupBase, c, m := checkSequence(p.producers, req)
		if c != protocol.ErrNone {
			return -1, -1, 0, c, m
		}
		if dupBase >= 0 {
			return dupBase, dupBase + int64(len(req.Records)), p.leaderEpoch, protocol.ErrNone, "duplicate"
		}
	}

	now := time.Now().UnixMilli()
	recs := req.Records
	for i := range recs {
		recs[i].LeaderEpoch = p.leaderEpoch
		recs[i].ProducerID = req.ProducerID
		if req.ProducerID >= 0 {
			recs[i].Sequence = req.BaseSequence + int32(i)
		} else {
			recs[i].Sequence = -1
		}
		if recs[i].Timestamp <= 0 {
			recs[i].Timestamp = now
		}
	}
	base, err := p.log.Append(recs)
	if err != nil {
		return -1, -1, 0, protocol.ErrUnknown, err.Error()
	}
	end = base + int64(len(recs))
	if req.ProducerID >= 0 {
		recordSequence(p.producers, req.ProducerID, req.BaseSequence, int32(len(recs)), base)
	}
	p.maybeAdvanceHWLocked()
	go p.wakeWaiters()
	return base, end, p.leaderEpoch, protocol.ErrNone, ""
}

// waitForHW blocks until HW >= target (acks=all), leadership is lost, or
// ctx expires.
func (p *Partition) waitForHW(ctx context.Context, target int64, epoch int32) protocol.ErrorCode {
	ch := make(chan struct{}, 1)
	p.addWaiter(ch)
	defer p.removeWaiter(ch)
	for {
		p.mu.Lock()
		hw, r, e := p.hw, p.role, p.leaderEpoch
		p.mu.Unlock()
		if hw >= target {
			return protocol.ErrNone
		}
		if r != roleLeader || e != epoch {
			// We lost leadership before the batch was committed. It may or
			// may not survive; the producer must retry (idempotence makes
			// that safe).
			return protocol.ErrNotLeader
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return protocol.ErrRequestTimedOut
		}
	}
}

// maybeAdvanceHWLocked recomputes HW = min LEO over the (maximal) ISR.
// The HW never moves backwards on a leader.
func (p *Partition) maybeAdvanceHWLocked() {
	if p.role != roleLeader {
		return
	}
	newHW := p.log.EndOffset()
	for _, r := range p.maximalISRLocked() {
		if r == p.self {
			continue
		}
		f, ok := p.followers[r]
		if !ok || f.leo < 0 {
			return // unknown follower position: cannot advance
		}
		if f.leo < newHW {
			newHW = f.leo
		}
	}
	if newHW > p.hw {
		p.hw = newHW
		go p.wakeWaiters()
	}
}

func (p *Partition) maximalISRLocked() []int32 {
	if p.pendingISR == nil {
		return p.isr
	}
	out := slices.Clone(p.isr)
	for _, r := range p.pendingISR {
		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------- reads

type readResult struct {
	err      protocol.ErrorCode
	records  []storage.Record
	hw       int64
	logStart int64
	logEnd   int64
	epoch    int32
	bytes    int
}

// read serves consumer and follower fetches. Consumers see records below
// the HW only; followers may read up to the log end. For followers the
// fetch offset doubles as an acknowledgement: "I have everything before
// fetchOffset", which is how the leader learns follower LEOs.
func (p *Partition) read(replicaID int32, offset int64, maxBytes int, fetcherEpoch int32) readResult {
	p.mu.Lock()
	if p.role != roleLeader {
		res := readResult{err: protocol.ErrNotLeader, epoch: p.leaderEpoch}
		p.mu.Unlock()
		return res
	}
	if fetcherEpoch >= 0 && fetcherEpoch != p.leaderEpoch {
		code := protocol.ErrFencedLeaderEpoch
		if fetcherEpoch > p.leaderEpoch {
			code = protocol.ErrUnknownLeaderEpoch
		}
		res := readResult{err: code, epoch: p.leaderEpoch}
		p.mu.Unlock()
		return res
	}
	maxOffset := p.hw
	if replicaID >= 0 {
		p.updateFollowerLocked(replicaID, offset)
		maxOffset = -1
	}
	hw, epoch := p.hw, p.leaderEpoch
	p.mu.Unlock()

	recs, err := p.log.Read(offset, maxOffset, maxBytes)
	res := readResult{hw: hw, logStart: p.log.StartOffset(), logEnd: p.log.EndOffset(), epoch: epoch}
	if err != nil {
		if errors.Is(err, storage.ErrOffsetOutOfRange) {
			res.err = protocol.ErrOffsetOutOfRange
		} else {
			res.err = protocol.ErrUnknown
		}
		return res
	}
	res.records = recs
	for i := range recs {
		res.bytes += recs[i].EncodedSize()
	}
	return res
}

// updateFollowerLocked records a follower's progress and may advance the
// HW or (Phase 3) trigger an ISR expansion.
func (p *Partition) updateFollowerLocked(replicaID int32, fetchOffset int64) {
	f, ok := p.followers[replicaID]
	if !ok {
		return // not a replica of this partition
	}
	now := time.Now()
	leaderLEO := p.log.EndOffset()
	// "Caught up" (Kafka's rule): either the follower has everything we
	// have right now, or it has everything we had when it fetched last
	// time. The second case matters under constant load, where a healthy
	// follower is always a few records behind the live end of the log.
	switch {
	case fetchOffset >= leaderLEO:
		f.lastCaughtUpTime = now
	case fetchOffset >= f.lastFetchLeaderLEO && !f.lastFetchTime.IsZero():
		f.lastCaughtUpTime = f.lastFetchTime
	}
	f.leo = fetchOffset
	f.lastFetchTime = now
	f.lastFetchLeaderLEO = leaderLEO
	p.maybeAdvanceHWLocked()
}

// close closes the log.
func (p *Partition) close() error { return p.log.Close() }

// Dir returns the on-disk directory of this replica's log.
func (p *Partition) Dir() string { return p.log.Dir() }

// ReadAll returns every record in the local log (tests and tooling).
func (p *Partition) ReadAll() ([]storage.Record, error) {
	var out []storage.Record
	off := p.log.StartOffset()
	for {
		recs, err := p.log.Read(off, -1, 4<<20)
		if err != nil {
			return out, err
		}
		if len(recs) == 0 {
			return out, nil
		}
		out = append(out, recs...)
		off = recs[len(recs)-1].Offset + 1
	}
}
