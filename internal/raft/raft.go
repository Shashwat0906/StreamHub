// Package raft is a small Raft implementation (leader election, log
// replication, durable state) used for StreamHub's cluster metadata.
//
// It follows the Raft paper (Ongaro & Ousterhout, figure 2) closely and
// adds two well-known extensions that matter for a broker:
//
//   - A no-op entry on election, so entries from earlier terms get committed
//     (paper §8).
//   - Check-quorum: a leader that cannot reach a majority for an election
//     timeout steps down. Without it an isolated leader would keep calling
//     itself controller and keep its brokers believing they are healthy.
//
// Deliberately NOT implemented (see PLAN.md §2): snapshots/log compaction,
// membership changes, pre-vote, linearizable follower reads.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// ErrNotLeader is returned by Propose on a follower or when leadership is
// lost before the entry is applied (the entry may still commit later).
var ErrNotLeader = errors.New("raft: not leader")

// ErrStopped is returned after Close.
var ErrStopped = errors.New("raft: stopped")

// FSM is the replicated state machine. Apply is called exactly once per
// committed entry, in log order, on every node.
type FSM interface {
	Apply(index uint64, data []byte) any
}

// RPC sends Raft messages to peers.
type RPC interface {
	Vote(ctx context.Context, addr string, req *protocol.RaftVoteRequest) (*protocol.RaftVoteResponse, error)
	Append(ctx context.Context, addr string, req *protocol.RaftAppendRequest) (*protocol.RaftAppendResponse, error)
}

// Config configures a Node.
type Config struct {
	ID                int32
	Peers             map[int32]string // all voters including self
	Dir               string
	ElectionTimeout   time.Duration // minimum; actual is random in [T, 2T)
	HeartbeatInterval time.Duration
	RPC               RPC
	FSM               FSM
	Logger            *slog.Logger
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

type proposal struct {
	term uint64
	ch   chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

// Node is one Raft participant.
type Node struct {
	cfg    Config
	logger *slog.Logger

	mu               sync.Mutex
	hs               hardState
	log              *diskLog
	role             role
	leaderID         int32
	commitIndex      uint64
	lastApplied      uint64
	leaderCommitSeen uint64
	electionDeadline time.Time
	leaderSince      time.Time
	nextIndex        map[int32]uint64
	matchIndex       map[int32]uint64
	lastAck          map[int32]time.Time
	pending          map[uint64]*proposal

	replicateCh map[int32]chan struct{}
	applyCh     chan struct{}
	stopCh      chan struct{}
	stopped     bool
	wg          sync.WaitGroup
}

// Start opens persistent state and starts the node's goroutines.
func Start(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 600 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 100 * time.Millisecond
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, errors.New("raft: peers must include self")
	}
	l, err := openDiskLog(cfg.Dir)
	if err != nil {
		return nil, err
	}
	hs, err := loadHardState(cfg.Dir)
	if err != nil {
		l.close()
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		logger:      cfg.Logger.With("component", "raft"),
		hs:          hs,
		log:         l,
		leaderID:    -1,
		nextIndex:   map[int32]uint64{},
		matchIndex:  map[int32]uint64{},
		lastAck:     map[int32]time.Time{},
		pending:     map[uint64]*proposal{},
		replicateCh: map[int32]chan struct{}{},
		applyCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	n.resetElectionDeadlineLocked()
	n.logger.Info("raft node started", "term", hs.Term, "voted_for", hs.VotedFor,
		"last_index", l.lastIndex(), "peers", len(cfg.Peers))

	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		ch := make(chan struct{}, 1)
		n.replicateCh[id] = ch
		n.wg.Add(1)
		go n.replicator(id, addr, ch)
	}
	n.wg.Add(2)
	go n.ticker()
	go n.applier()
	return n, nil
}

// Close stops the node. Pending proposals fail with ErrStopped.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.stopped = true
	for idx, p := range n.pending {
		p.ch <- proposalResult{err: ErrStopped}
		delete(n.pending, idx)
	}
	close(n.stopCh)
	n.mu.Unlock()
	n.wg.Wait()
	return n.log.close()
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

// IsLeader reports whether this node currently believes it is leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == leader
}

// LeaderID is the last known leader (-1 if unknown).
func (n *Node) LeaderID() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.hs.Term
}

// CaughtUp reports whether this node knows a leader and has applied
// everything that leader had committed when it last heard from it.
func (n *Node) CaughtUp() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID < 0 {
		return false
	}
	target := n.leaderCommitSeen
	if n.role == leader {
		target = n.commitIndex
	}
	return n.commitIndex > 0 && n.lastApplied >= target
}

// Status is a diagnostic snapshot.
type Status struct {
	ID          int32
	Role        string
	Term        uint64
	LeaderID    int32
	LastIndex   uint64
	CommitIndex uint64
	LastApplied uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{n.cfg.ID, n.role.String(), n.hs.Term, n.leaderID, n.log.lastIndex(), n.commitIndex, n.lastApplied}
}

// ---------------------------------------------------------------- timers

func (n *Node) resetElectionDeadlineLocked() {
	t := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(t + time.Duration(rand.Int63n(int64(t))))
}

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		now := time.Now()
		switch {
		case n.role != leader && now.After(n.electionDeadline):
			n.startElectionLocked()
		case n.role == leader && len(n.cfg.Peers) > 1:
			n.checkQuorumLocked(now)
		}
		n.mu.Unlock()
	}
}

// checkQuorumLocked steps down a leader that has not heard from a majority
// within one (maximum) election timeout.
func (n *Node) checkQuorumLocked(now time.Time) {
	window := 2 * n.cfg.ElectionTimeout
	if now.Sub(n.leaderSince) < window {
		return
	}
	reachable := 1
	for id, t := range n.lastAck {
		if id != n.cfg.ID && now.Sub(t) < window {
			reachable++
		}
	}
	if reachable < n.quorum() {
		n.logger.Warn("leader lost quorum, stepping down", "term", n.hs.Term, "reachable", reachable)
		n.becomeFollowerLocked(n.hs.Term, -1)
	}
}

// ---------------------------------------------------------------- elections

func (n *Node) persistLocked() {
	if err := saveHardState(n.cfg.Dir, n.hs); err != nil {
		// Without durable term/vote we could vote twice in a term, which
		// breaks safety. Crash loudly rather than continue.
		panic("raft: cannot persist hard state: " + err.Error())
	}
}

func (n *Node) startElectionLocked() {
	n.role = candidate
	n.hs.Term++
	n.hs.VotedFor = n.cfg.ID
	n.leaderID = -1
	n.persistLocked()
	n.resetElectionDeadlineLocked()
	term := n.hs.Term
	req := &protocol.RaftVoteRequest{
		Term: term, CandidateID: n.cfg.ID,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(),
	}
	n.logger.Info("starting election", "term", term)
	if n.quorum() == 1 {
		n.becomeLeaderLocked()
		return
	}
	votes := 1
	for id, addr := range n.cfg.Peers {
		if id == n.cfg.ID {
			continue
		}
		go func(id int32, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.cfg.RPC.Vote(ctx, addr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped || n.hs.Term != term || n.role != candidate {
				return
			}
			if resp.Term > n.hs.Term {
				n.becomeFollowerLocked(resp.Term, -1)
				return
			}
			if resp.Granted {
				votes++
				if votes >= n.quorum() {
					n.becomeLeaderLocked()
				}
			}
		}(id, addr)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = leader
	n.leaderID = n.cfg.ID
	n.leaderSince = time.Now()
	last := n.log.lastIndex()
	for id := range n.cfg.Peers {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
		n.lastAck[id] = n.leaderSince
	}
	n.logger.Info("became leader", "term", n.hs.Term, "last_index", last)
	// A no-op in the new term lets us commit entries from older terms.
	if err := n.log.append(Entry{Term: n.hs.Term, Index: last + 1}); err != nil {
		panic("raft: cannot append: " + err.Error())
	}
	n.matchIndex[n.cfg.ID] = last + 1
	n.maybeCommitLocked()
	n.signalReplicatorsLocked()
}

// becomeFollowerLocked moves to follower in term (persisting a new term).
func (n *Node) becomeFollowerLocked(term uint64, leaderID int32) {
	if term > n.hs.Term {
		n.hs.Term = term
		n.hs.VotedFor = -1
		n.persistLocked()
	}
	if n.role == leader {
		n.logger.Info("stepped down", "term", n.hs.Term)
	}
	n.role = follower
	n.leaderID = leaderID
	n.resetElectionDeadlineLocked()
	n.failPendingLocked(ErrNotLeader)
}

func (n *Node) failPendingLocked(err error) {
	for idx, p := range n.pending {
		p.ch <- proposalResult{err: err}
		delete(n.pending, idx)
	}
}

// HandleVote answers a RequestVote RPC.
func (n *Node) HandleVote(req *protocol.RaftVoteRequest) *protocol.RaftVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.hs.Term {
		return &protocol.RaftVoteResponse{Term: n.hs.Term}
	}
	if req.Term > n.hs.Term {
		n.becomeFollowerLocked(req.Term, -1)
	}
	// Election restriction (§5.4.1): only vote for a candidate whose log is
	// at least as up to date as ours, so a new leader has every committed
	// entry.
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if (n.hs.VotedFor == -1 || n.hs.VotedFor == req.CandidateID) && upToDate {
		n.hs.VotedFor = req.CandidateID
		n.persistLocked()
		n.resetElectionDeadlineLocked()
		return &protocol.RaftVoteResponse{Term: n.hs.Term, Granted: true}
	}
	return &protocol.RaftVoteResponse{Term: n.hs.Term}
}

// ---------------------------------------------------------------- replication

// HandleAppend answers an AppendEntries RPC (also used as heartbeat).
func (n *Node) HandleAppend(req *protocol.RaftAppendRequest) *protocol.RaftAppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.hs.Term {
		return &protocol.RaftAppendResponse{Term: n.hs.Term}
	}
	if req.Term > n.hs.Term || n.role != follower || n.leaderID != req.LeaderID {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.resetElectionDeadlineLocked()

	// Log matching: our entry at PrevLogIndex must have PrevLogTerm.
	if req.PrevLogIndex > n.log.lastIndex() {
		return &protocol.RaftAppendResponse{Term: n.hs.Term, ConflictIndex: n.log.lastIndex() + 1}
	}
	if t, _ := n.log.term(req.PrevLogIndex); t != req.PrevLogTerm {
		// Skip back over the whole conflicting term in one round trip.
		ci := req.PrevLogIndex
		for ci > 1 {
			if pt, _ := n.log.term(ci - 1); pt != t {
				break
			}
			ci--
		}
		return &protocol.RaftAppendResponse{Term: n.hs.Term, ConflictIndex: max(ci, 1)}
	}

	var toAppend []Entry
	for i, e := range req.Entries {
		if e.Index <= n.log.lastIndex() {
			if t, _ := n.log.term(e.Index); t == e.Term {
				continue // already have it
			}
			// Conflict: delete it and everything after (§5.3). Never
			// happens for committed entries thanks to the election rule.
			if e.Index <= n.commitIndex {
				panic("raft: leader asked to overwrite a committed entry")
			}
			if err := n.log.truncateFrom(e.Index); err != nil {
				panic("raft: truncate failed: " + err.Error())
			}
		}
		for _, rest := range req.Entries[i:] {
			toAppend = append(toAppend, Entry{Term: rest.Term, Index: rest.Index, Data: rest.Data})
		}
		break
	}
	if len(toAppend) > 0 {
		if err := n.log.append(toAppend...); err != nil {
			panic("raft: append failed: " + err.Error())
		}
	}
	lastNew := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > n.leaderCommitSeen {
		n.leaderCommitSeen = req.LeaderCommit
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, lastNew)
		n.signalApplyLocked()
	}
	return &protocol.RaftAppendResponse{Term: n.hs.Term, Success: true, MatchIndex: lastNew}
}

func (n *Node) signalReplicatorsLocked() {
	for _, ch := range n.replicateCh {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Node) signalApplyLocked() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// replicator sends AppendEntries to one peer while we are leader: on new
// entries, and every heartbeat interval otherwise.
func (n *Node) replicator(peer int32, addr string, wake chan struct{}) {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-wake:
		case <-t.C:
		}
		for i := 0; i < 64; i++ { // bounded catch-up burst
			if !n.sendAppend(peer, addr) {
				break
			}
		}
	}
}

// sendAppend sends one AppendEntries and returns true if more should be
// sent immediately (follower behind or log mismatch).
func (n *Node) sendAppend(peer int32, addr string) bool {
	n.mu.Lock()
	if n.role != leader || n.stopped {
		n.mu.Unlock()
		return false
	}
	term := n.hs.Term
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm, _ := n.log.term(prevIdx)
	entries := n.log.slice(next, 256)
	req := &protocol.RaftAppendRequest{
		Term: term, LeaderID: n.cfg.ID,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, LeaderCommit: n.commitIndex,
	}
	for _, e := range entries {
		req.Entries = append(req.Entries, protocol.RaftEntry{Term: e.Term, Index: e.Index, Data: e.Data})
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.HeartbeatInterval+200*time.Millisecond)
	resp, err := n.cfg.RPC.Append(ctx, addr, req)
	cancel()
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != leader || n.hs.Term != term {
		return false
	}
	if resp.Term > n.hs.Term {
		n.becomeFollowerLocked(resp.Term, -1)
		return false
	}
	n.lastAck[peer] = time.Now()
	if resp.Success {
		if resp.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = resp.MatchIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeCommitLocked()
		return n.nextIndex[peer] <= n.log.lastIndex()
	}
	ci := resp.ConflictIndex
	if ci < 1 {
		ci = 1
	}
	if ci >= next {
		ci = next - 1
		if ci < 1 {
			ci = 1
		}
	}
	n.nextIndex[peer] = ci
	return true
}

// maybeCommitLocked advances commitIndex to the highest index stored on a
// majority, but only for entries of the current term (§5.4.2).
func (n *Node) maybeCommitLocked() {
	n.matchIndex[n.cfg.ID] = n.log.lastIndex()
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for id := range n.cfg.Peers {
		matches = append(matches, n.matchIndex[id])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1] // highest index held by a majority
	if candidate <= n.commitIndex {
		return
	}
	if t, _ := n.log.term(candidate); t != n.hs.Term {
		return
	}
	n.commitIndex = candidate
	n.signalApplyLocked()
}

// ---------------------------------------------------------------- proposals & apply

// Propose appends data to the log and waits until it is committed and
// applied locally; it returns the FSM's result.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	if data == nil {
		data = []byte{}
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	e := Entry{Term: n.hs.Term, Index: n.log.lastIndex() + 1, Data: data}
	if err := n.log.append(e); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	p := &proposal{term: e.Term, ch: make(chan proposalResult, 1)}
	n.pending[e.Index] = p
	n.maybeCommitLocked()
	n.signalReplicatorsLocked()
	n.mu.Unlock()

	select {
	case r := <-p.ch:
		return r.value, r.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pending, e.Index)
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}
		for {
			n.mu.Lock()
			if n.stopped || n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			batch := n.log.slice(n.lastApplied+1, int(min(n.commitIndex-n.lastApplied, 512)))
			n.mu.Unlock()
			for _, e := range batch {
				var result any
				if e.Data != nil {
					result = n.cfg.FSM.Apply(e.Index, e.Data)
				}
				n.mu.Lock()
				n.lastApplied = e.Index
				if p, ok := n.pending[e.Index]; ok {
					if p.term == e.Term {
						p.ch <- proposalResult{value: result}
					} else {
						p.ch <- proposalResult{err: ErrNotLeader} // overwritten by another leader
					}
					delete(n.pending, e.Index)
				}
				n.mu.Unlock()
			}
		}
	}
}
