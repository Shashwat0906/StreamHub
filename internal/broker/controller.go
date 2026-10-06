package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/raft"
)

// ---------------------------------------------------------------- raft glue

// raftProposer adapts raft.Node to metadata.Proposer.
type raftProposer struct{ n *raft.Node }

func (p raftProposer) Propose(ctx context.Context, cmd metadata.Command) (metadata.Result, error) {
	v, err := p.n.Propose(ctx, cmd.Encode())
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return metadata.Result{}, metadata.ErrNotLeader
		}
		return metadata.Result{}, err
	}
	res, _ := v.(metadata.Result)
	return res, nil
}

func (p raftProposer) IsLeader() bool  { return p.n.IsLeader() }
func (p raftProposer) LeaderID() int32 { return p.n.LeaderID() }

// raftRPC sends Raft messages over the broker protocol.
type raftRPC struct{ b *Broker }

func (r raftRPC) Vote(ctx context.Context, addr string, req *protocol.RaftVoteRequest) (*protocol.RaftVoteResponse, error) {
	var resp protocol.RaftVoteResponse
	if err := r.b.pool.Call(ctx, addr, protocol.APIRaftVote, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r raftRPC) Append(ctx context.Context, addr string, req *protocol.RaftAppendRequest) (*protocol.RaftAppendResponse, error) {
	var resp protocol.RaftAppendResponse
	if err := r.b.pool.Call(ctx, addr, protocol.APIRaftAppend, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// startRaft starts the metadata quorum member. Every broker in Peers is a
// voter; the Raft leader is the active controller.
func (b *Broker) startRaft() error {
	if _, ok := b.cfg.Peers[b.cfg.ID]; !ok {
		return errors.New("broker: --peers must include this broker's own ID")
	}
	node, err := raft.Start(raft.Config{
		ID:                b.cfg.ID,
		Peers:             b.cfg.Peers,
		Dir:               filepath.Join(b.cfg.DataDir, "raft"),
		ElectionTimeout:   b.cfg.RaftElectionMin,
		HeartbeatInterval: b.cfg.RaftHeartbeat,
		RPC:               raftRPC{b},
		FSM:               b.meta,
		Logger:            b.logger,
	})
	if err != nil {
		return err
	}
	b.raft = node
	b.proposer = raftProposer{node}
	b.closers = append(b.closers, node.Close)
	b.controller = newController(b)
	b.goLoop(b.cfg.HeartbeatInterval, b.controller.tick)
	b.goLoop(b.cfg.HeartbeatInterval, b.sendHeartbeat)
	if b.cfg.PreferredLeaderInterval > 0 {
		b.goLoop(b.cfg.PreferredLeaderInterval, b.controller.preferredLeaderTick)
	}
	return nil
}

// metadataReady reports whether the local metadata image is current enough
// to act on (Raft mode: we have replayed what the leader had committed).
func (b *Broker) metadataReady() bool {
	if b.raft == nil {
		return true
	}
	return b.raft.CaughtUp()
}

// ---------------------------------------------------------------- broker side

// sendHeartbeat tells the controller we are alive. A successful heartbeat
// is also what keeps this broker allowed to accept writes as a leader.
func (b *Broker) sendHeartbeat() {
	leader := b.proposer.LeaderID()
	if leader < 0 {
		return
	}
	req := &protocol.BrokerHeartbeatRequest{BrokerID: b.cfg.ID, Addr: b.addr, HTTPAddr: b.advertisedHTTP}
	var resp *protocol.BrokerHeartbeatResponse
	if leader == b.cfg.ID {
		resp = b.controller.onHeartbeat(b.ctx, req)
	} else {
		addr, ok := b.brokerAddr(leader)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(b.ctx, b.cfg.SessionTimeout/2)
		defer cancel()
		var r protocol.BrokerHeartbeatResponse
		if err := b.pool.Call(ctx, addr, protocol.APIBrokerHeartbeat, req, &r); err != nil {
			b.logger.Debug("heartbeat failed", "controller", leader, "err", err)
			return
		}
		resp = &r
	}
	if resp.Err == protocol.ErrNone && !resp.Fenced {
		b.lastHeartbeatOK.Store(time.Now().UnixNano())
	}
}

// isFenced reports whether this broker must refuse leader writes because
// it has not had a successful controller heartbeat recently.
//
// Why: if we are cut off from the controller, the controller will fence us
// after SessionTimeout and elect new leaders for our partitions. We stop
// accepting writes a bit earlier (3/4 of the session timeout) so that there
// is no window in which two brokers both accept writes for one partition.
func (b *Broker) isFenced() bool {
	if b.raft == nil {
		return false
	}
	last := b.lastHeartbeatOK.Load()
	if last == 0 {
		return true
	}
	return time.Since(time.Unix(0, last)) > b.cfg.SessionTimeout*3/4
}

// ---------------------------------------------------------------- controller

// controller runs on the Raft leader. It tracks broker liveness from
// heartbeats (in memory: liveness is not worth a log entry) and turns a
// missed session into a committed FenceBroker command; the metadata state
// machine then elects new partition leaders from the ISR.
type controller struct {
	b         *Broker
	mu        sync.Mutex
	lastSeen  map[int32]time.Time
	active    bool // we were leader on the previous tick
	proposing atomic.Bool
}

func newController(b *Broker) *controller {
	return &controller{b: b, lastSeen: map[int32]time.Time{}}
}

func (c *controller) onHeartbeat(ctx context.Context, req *protocol.BrokerHeartbeatRequest) *protocol.BrokerHeartbeatResponse {
	b := c.b
	if !b.proposer.IsLeader() {
		return &protocol.BrokerHeartbeatResponse{Err: protocol.ErrNotController, ControllerID: b.proposer.LeaderID()}
	}
	if req.ShuttingDown {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		res, err := b.proposer.Propose(pctx, metadata.Command{Type: metadata.CmdFenceBroker, BrokerID: req.BrokerID})
		if err != nil {
			return &protocol.BrokerHeartbeatResponse{Err: protocol.ErrNotController, ControllerID: b.proposer.LeaderID()}
		}
		b.logger.Info("controlled shutdown: broker fenced", "broker_id", req.BrokerID)
		return &protocol.BrokerHeartbeatResponse{Err: res.Err, ControllerID: b.cfg.ID, Fenced: true}
	}
	c.mu.Lock()
	c.lastSeen[req.BrokerID] = time.Now()
	c.mu.Unlock()

	bm, ok := b.meta.Broker(req.BrokerID)
	if !ok || bm.Fenced || bm.Addr != req.Addr || bm.HTTPAddr != req.HTTPAddr {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		res, err := b.proposer.Propose(pctx, metadata.Command{Type: metadata.CmdRegisterBroker, BrokerID: req.BrokerID, Addr: req.Addr, HTTPAddr: req.HTTPAddr})
		if err != nil {
			return &protocol.BrokerHeartbeatResponse{Err: protocol.ErrNotController, ControllerID: b.proposer.LeaderID(), Fenced: true}
		}
		if res.Err != protocol.ErrNone {
			return &protocol.BrokerHeartbeatResponse{Err: res.Err, ControllerID: b.cfg.ID, Fenced: true}
		}
		b.logger.Info("broker registered/unfenced", "broker_id", req.BrokerID, "addr", req.Addr)
	}
	return &protocol.BrokerHeartbeatResponse{ControllerID: b.cfg.ID}
}

// tick fences brokers whose session expired.
func (c *controller) tick() {
	b := c.b
	if !b.proposer.IsLeader() {
		c.mu.Lock()
		c.active = false
		c.mu.Unlock()
		return
	}
	if !b.metadataReady() {
		return
	}
	now := time.Now()
	c.mu.Lock()
	if !c.active {
		// Newly elected controller: we have no liveness history. Give every
		// broker a full session before judging it.
		c.active = true
		for _, bm := range b.meta.Brokers() {
			c.lastSeen[bm.ID] = now
		}
		c.mu.Unlock()
		b.logger.Info("became active controller", "term", b.raft.Term())
		return
	}
	var expired []int32
	for _, bm := range b.meta.Brokers() {
		if bm.Fenced {
			continue
		}
		if seen, ok := c.lastSeen[bm.ID]; !ok || now.Sub(seen) > b.cfg.SessionTimeout {
			expired = append(expired, bm.ID)
		}
	}
	c.mu.Unlock()
	if len(expired) == 0 || !c.proposing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.proposing.Store(false)
		for _, id := range expired {
			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			res, err := b.proposer.Propose(ctx, metadata.Command{Type: metadata.CmdFenceBroker, BrokerID: id})
			cancel()
			if err == nil && res.Err == protocol.ErrNone {
				b.metrics.elections.Inc()
				b.logger.Warn("fenced broker after missed heartbeats", "broker_id", id, "session_timeout", b.cfg.SessionTimeout)
			}
		}
	}()
}

// ---------------------------------------------------------------- AlterISR

func (c *controller) onAlterISR(ctx context.Context, req *protocol.AlterISRRequest) *protocol.SimpleResponse {
	b := c.b
	if !b.proposer.IsLeader() {
		return &protocol.SimpleResponse{Err: protocol.ErrNotController}
	}
	res, err := b.proposer.Propose(ctx, metadata.Command{
		Type: metadata.CmdAlterISR, BrokerID: req.BrokerID, Topic: req.Topic,
		Partition: req.Partition, LeaderEpoch: req.LeaderEpoch, ISR: req.NewISR,
	})
	if err != nil {
		return &protocol.SimpleResponse{Err: protocol.ErrNotController, ErrMsg: err.Error()}
	}
	return &protocol.SimpleResponse{Err: res.Err, ErrMsg: res.Msg}
}

// preferredLeaderTick moves leadership back to each partition's preferred
// replica (Replicas[0]) when it is alive and in the ISR. Without this, a
// broker that restarts after a failure never leads anything again and the
// load concentrates on the survivors.
func (c *controller) preferredLeaderTick() {
	b := c.b
	if !b.proposer.IsLeader() || !b.metadataReady() {
		return
	}
	c.mu.Lock()
	active := c.active
	c.mu.Unlock()
	if !active || !c.proposing.CompareAndSwap(false, true) {
		return
	}
	defer c.proposing.Store(false)
	moved := 0
	for _, t := range b.meta.Topics() {
		for _, p := range t.Partitions {
			pref := p.Replicas[0]
			if p.Leader == pref || !slices.Contains(p.ISR, pref) {
				continue
			}
			if bm, ok := b.meta.Broker(pref); !ok || bm.Fenced {
				continue
			}
			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			res, err := b.proposer.Propose(ctx, metadata.Command{Type: metadata.CmdElectLeader, Topic: t.Name, Partition: p.ID, BrokerID: pref})
			cancel()
			if err != nil {
				return
			}
			if res.Err == protocol.ErrNone {
				moved++
				b.logger.Info("preferred leader elected", "partition", fmt.Sprintf("%s-%d", t.Name, p.ID), "from", p.Leader, "to", pref)
			}
			if moved >= 20 { // small steps: avoid moving everything at once
				return
			}
		}
	}
}

// controlledShutdown asks the controller to fence this broker so leaders
// move to other ISR members right away instead of after SessionTimeout.
// Best effort: if it fails, the normal session timeout still applies.
func (b *Broker) controlledShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &protocol.BrokerHeartbeatRequest{BrokerID: b.cfg.ID, Addr: b.addr, ShuttingDown: true}
	for ctx.Err() == nil {
		leader := b.proposer.LeaderID()
		if leader == b.cfg.ID {
			if resp := b.controller.onHeartbeat(ctx, req); resp.Err == protocol.ErrNone {
				return
			}
		} else if addr, ok := b.brokerAddr(leader); ok {
			var resp protocol.BrokerHeartbeatResponse
			if err := b.pool.Call(ctx, addr, protocol.APIBrokerHeartbeat, req, &resp); err == nil && resp.Err == protocol.ErrNone {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	b.logger.Warn("controlled shutdown did not complete; peers will notice via session timeout")
}
