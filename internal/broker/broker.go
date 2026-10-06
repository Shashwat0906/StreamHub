// Package broker is a StreamHub broker node: it serves the wire protocol,
// hosts partition replicas, takes part in the metadata quorum (and acts as
// controller when it is the Raft leader) and coordinates consumer groups.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/raft"
	"github.com/Shashwat0906/StreamHub/internal/storage"
	"github.com/Shashwat0906/StreamHub/internal/transport"
)

// Config configures one broker.
type Config struct {
	ID         int32
	ListenAddr string // host:port for the binary protocol
	// AdvertisedAddr is what other brokers and clients dial. Defaults to
	// the actual listen address.
	AdvertisedAddr string
	HTTPAddr       string // host:port for /metrics and /healthz ("" = disabled)
	// AdvertisedHTTPAddr is the HTTP address published in cluster metadata
	// (used by the dashboard). Defaults to the listen address, with an
	// unspecified host (0.0.0.0) replaced by the protocol address host.
	AdvertisedHTTPAddr string
	DataDir            string
	// Peers lists every metadata-quorum voter (including this broker) as
	// id -> address. Empty means standalone mode (single node, no Raft).
	Peers map[int32]string

	Log storage.Config

	// MinInsyncReplicas is the default min.insync.replicas for acks=all.
	MinInsyncReplicas int
	// DefaultReplicationFactor/Partitions for CreateTopic requests that
	// pass 0 / -1.
	DefaultPartitions        int32
	DefaultReplicationFactor int16

	// Liveness timings.
	HeartbeatInterval time.Duration // broker -> controller
	SessionTimeout    time.Duration // controller fences a broker after this
	ReplicaLagTime    time.Duration // follower dropped from ISR after this
	ReplicaFetchWait  time.Duration // follower long-poll wait
	FlushInterval     time.Duration // periodic fsync + HW checkpoint
	RetentionCheck    time.Duration
	RaftElectionMin   time.Duration
	RaftHeartbeat     time.Duration
	GroupInitialDelay time.Duration // wait for more members before the first assignment
	OffsetsPartitions int32         // partitions of __consumer_offsets
	// PreferredLeaderInterval: how often the controller moves leadership
	// back to preferred replicas (0 = default 30s, negative = disabled).
	PreferredLeaderInterval time.Duration
	// MaxMessageBytes caps one record's key+value size (default 1 MiB);
	// topic config max.message.bytes overrides it.
	MaxMessageBytes    int
	OffsetsReplication int16

	Faults *transport.Faults // fault injection (tests only)
	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.MinInsyncReplicas == 0 {
		c.MinInsyncReplicas = 1
	}
	if c.DefaultPartitions == 0 {
		c.DefaultPartitions = 3
	}
	if c.DefaultReplicationFactor == 0 {
		c.DefaultReplicationFactor = 1
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 500 * time.Millisecond
	}
	if c.SessionTimeout == 0 {
		c.SessionTimeout = 3 * time.Second
	}
	if c.ReplicaLagTime == 0 {
		c.ReplicaLagTime = 5 * time.Second
	}
	if c.ReplicaFetchWait == 0 {
		c.ReplicaFetchWait = 250 * time.Millisecond
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
	if c.RetentionCheck == 0 {
		c.RetentionCheck = 30 * time.Second
	}
	if c.RaftElectionMin == 0 {
		c.RaftElectionMin = 600 * time.Millisecond
	}
	if c.RaftHeartbeat == 0 {
		c.RaftHeartbeat = 100 * time.Millisecond
	}
	if c.GroupInitialDelay == 0 {
		c.GroupInitialDelay = 300 * time.Millisecond
	}
	if c.PreferredLeaderInterval == 0 {
		c.PreferredLeaderInterval = 30 * time.Second
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = 1 << 20
	}
	if c.OffsetsPartitions == 0 {
		c.OffsetsPartitions = 8
	}
	if c.OffsetsReplication == 0 {
		c.OffsetsReplication = 3
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Log.SegmentBytes == 0 {
		c.Log = storage.DefaultConfig()
	}
}

// Broker is one StreamHub node.
type Broker struct {
	cfg    Config
	logger *slog.Logger

	server *transport.Server
	pool   *transport.Pool
	addr   string // advertised address

	meta       *metadata.Store
	proposer   metadata.Proposer
	raft       *raft.Node // nil in standalone mode
	controller *controller
	closers    []func() error

	lastHeartbeatOK atomic.Int64 // unix nanos of the last good controller heartbeat

	replicas       *ReplicaManager
	groups         *coordinator
	metrics        *brokerMetrics
	httpAddr       string
	advertisedHTTP string
	started        time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// initDone is closed once every component is wired up. The listener
	// must exist earlier (we need its address), so requests that arrive
	// during startup wait for this instead of seeing half-built state.
	initDone chan struct{}

	closeOnce sync.Once
}

// New creates and starts a broker.
func New(cfg Config) (*Broker, error) {
	cfg.setDefaults()
	if cfg.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		cfg:      cfg,
		logger:   cfg.Logger.With("broker", cfg.ID),
		meta:     metadata.NewStore(),
		metrics:  newBrokerMetrics(),
		ctx:      ctx,
		cancel:   cancel,
		initDone: make(chan struct{}),
		started:  time.Now(),
	}

	srv, err := transport.Listen(cfg.ListenAddr, b.handle, b.logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	b.server = srv
	b.addr = cfg.AdvertisedAddr
	if b.addr == "" {
		b.addr = srv.Addr()
	}
	b.pool = transport.NewPool(&transport.Dialer{Self: b.addr, Faults: cfg.Faults})

	// HTTP first: its advertised address is part of broker registration.
	if err := b.startHTTP(); err != nil {
		b.Close()
		return nil, err
	}
	if err := b.startMetadata(); err != nil {
		b.Close()
		return nil, err
	}

	b.groups = newCoordinator(b)
	b.goLoop(100*time.Millisecond, b.groups.tick)

	rm, err := newReplicaManager(b)
	if err != nil {
		b.Close()
		return nil, err
	}
	b.replicas = rm
	rm.start()
	b.registerGauges()

	close(b.initDone)
	b.logger.Info("broker started", "addr", b.addr, "data_dir", cfg.DataDir)
	return b, nil
}

// startMetadata opens the metadata log. Standalone mode (no peers) uses a
// local log; clustered mode uses Raft.
func (b *Broker) startMetadata() error {
	if len(b.cfg.Peers) > 0 {
		return b.startRaft()
	}
	path := filepath.Join(b.cfg.DataDir, "metadata.log")
	ll, err := metadata.OpenLocalLog(path, b.meta, b.cfg.ID)
	if err != nil {
		return err
	}
	b.proposer = ll
	b.closers = append(b.closers, ll.Close)
	// Register ourselves (standalone: we are our own controller).
	res, err := ll.Propose(b.ctx, metadata.Command{Type: metadata.CmdRegisterBroker, BrokerID: b.cfg.ID, Addr: b.addr, HTTPAddr: b.advertisedHTTP})
	if err != nil {
		return err
	}
	if res.Err != protocol.ErrNone {
		return res.Err.AsError(res.Msg)
	}
	return nil
}

// ID returns the broker ID.
func (b *Broker) ID() int32 { return b.cfg.ID }

// Addr returns the advertised protocol address.
func (b *Broker) Addr() string { return b.addr }

// Metadata exposes the metadata store (tests, diagnostics).
func (b *Broker) Metadata() *metadata.Store { return b.meta }

// Partition returns the local replica of a partition, if hosted here.
func (b *Broker) Partition(topic string, id int32) *Partition {
	return b.replicas.get(topic, id)
}

// Close shuts the broker down gracefully: in cluster mode it first asks
// the controller to move its leaderships elsewhere (controlled shutdown).
func (b *Broker) Close() error {
	if b.raft != nil && b.initDoneClosed() {
		b.controlledShutdown()
	}
	return b.shutdown()
}

// Kill stops the broker abruptly, like a crash: no controlled shutdown,
// peers only find out through missed heartbeats. (In-process we cannot
// drop the OS page cache, so data already written is still on disk.)
func (b *Broker) Kill() error { return b.shutdown() }

func (b *Broker) initDoneClosed() bool {
	select {
	case <-b.initDone:
		return true
	default:
		return false
	}
}

func (b *Broker) shutdown() error {
	b.closeOnce.Do(func() {
		b.cancel()
		if b.server != nil {
			b.server.Close()
		}
		b.wg.Wait()
		if b.replicas != nil {
			b.replicas.close()
		}
		for i := len(b.closers) - 1; i >= 0; i-- {
			b.closers[i]()
		}
		if b.pool != nil {
			b.pool.Close()
		}
		b.logger.Info("broker stopped")
	})
	return nil
}

// goLoop runs fn every interval until the broker stops.
func (b *Broker) goLoop(interval time.Duration, fn func()) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-t.C:
				fn()
			}
		}
	}()
}

// brokerAddr resolves a broker ID to its address via metadata.
func (b *Broker) brokerAddr(id int32) (string, bool) {
	if bm, ok := b.meta.Broker(id); ok && bm.Addr != "" {
		return bm.Addr, true
	}
	if addr, ok := b.cfg.Peers[id]; ok {
		return addr, true
	}
	return "", false
}
