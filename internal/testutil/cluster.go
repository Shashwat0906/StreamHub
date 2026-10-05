// Package testutil starts real StreamHub brokers in-process for
// integration and failure tests. Each broker has its own TCP port and data
// directory; network partitions are simulated with transport.Faults.
package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/transport"
)

// Cluster is a set of in-process brokers.
type Cluster struct {
	T       testing.TB
	Faults  *transport.Faults
	Brokers map[int32]*broker.Broker
	cfgs    map[int32]broker.Config
}

// FastTimings shortens every timeout so failure tests run in seconds.
func FastTimings(c *broker.Config) {
	c.HeartbeatInterval = 100 * time.Millisecond
	c.SessionTimeout = 1200 * time.Millisecond
	c.ReplicaLagTime = 1500 * time.Millisecond
	c.ReplicaFetchWait = 100 * time.Millisecond
	c.FlushInterval = 200 * time.Millisecond
	c.RaftElectionMin = 300 * time.Millisecond
	c.RaftHeartbeat = 50 * time.Millisecond
	c.GroupInitialDelay = 200 * time.Millisecond
}

func freePort(t testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// NewCluster starts n brokers (IDs 1..n). n == 1 runs standalone unless
// forceQuorum is set by an option. Options mutate each broker's config.
func NewCluster(t testing.TB, n int, opts ...func(*broker.Config)) *Cluster {
	t.Helper()
	c := &Cluster{T: t, Faults: transport.NewFaults(), Brokers: map[int32]*broker.Broker{}, cfgs: map[int32]broker.Config{}}
	root := t.TempDir()
	peers := map[int32]string{}
	for i := 1; i <= n; i++ {
		peers[int32(i)] = freePort(t)
	}
	level := slog.LevelWarn
	if os.Getenv("STREAMHUB_TEST_LOG") != "" {
		level = slog.LevelDebug
	}
	for i := 1; i <= n; i++ {
		id := int32(i)
		cfg := broker.Config{
			ID:         id,
			ListenAddr: peers[id],
			DataDir:    filepath.Join(root, fmt.Sprintf("broker-%d", id)),
			Faults:     c.Faults,
			Logger:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("test", t.Name()),
		}
		if n > 1 {
			cfg.Peers = peers
		}
		FastTimings(&cfg)
		for _, o := range opts {
			o(&cfg)
		}
		c.cfgs[id] = cfg
	}
	// Start all brokers concurrently: with Raft each one waits for a quorum.
	type started struct {
		id  int32
		b   *broker.Broker
		err error
	}
	ch := make(chan started, n)
	for id := range c.cfgs {
		go func(id int32) {
			b, err := broker.New(c.cfgs[id])
			ch <- started{id, b, err}
		}(id)
	}
	for i := 0; i < n; i++ {
		s := <-ch
		if s.err != nil {
			t.Fatalf("start broker %d: %v", s.id, s.err)
		}
		c.Brokers[s.id] = s.b
	}
	t.Cleanup(c.Close)
	return c
}

// IDs returns running broker IDs, sorted.
func (c *Cluster) IDs() []int32 {
	var ids []int32
	for id := range c.Brokers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Addr returns a broker's address (running or not).
func (c *Cluster) Addr(id int32) string { return c.cfgs[id].ListenAddr }

// Bootstrap returns all broker addresses.
func (c *Cluster) Bootstrap() []string {
	var out []string
	for id := int32(1); int(id) <= len(c.cfgs); id++ {
		out = append(out, c.cfgs[id].ListenAddr)
	}
	return out
}

// Client returns a new client connected to the cluster (closed on cleanup).
func (c *Cluster) Client() *client.Client {
	cl, err := client.New(client.Config{
		Bootstrap:      c.Bootstrap(),
		RequestTimeout: 5 * time.Second,
		MetadataMaxAge: 2 * time.Second,
		Dialer:         &transport.Dialer{Self: "client", Faults: c.Faults},
	})
	if err != nil {
		c.T.Fatal(err)
	}
	c.T.Cleanup(func() { cl.Close() })
	return cl
}

// Stop shuts a broker down cleanly.
func (c *Cluster) Stop(id int32) {
	if b, ok := c.Brokers[id]; ok {
		b.Close()
		delete(c.Brokers, id)
	}
}

// Kill is Stop: in-process we cannot SIGKILL, but Close does not run any
// extra handover logic (no controlled shutdown), so peers observe the
// same thing as a crash: connections drop and heartbeats stop.
func (c *Cluster) Kill(id int32) { c.Stop(id) }

// Start (re)starts a stopped broker with its original config and data dir.
func (c *Cluster) Start(id int32) *broker.Broker {
	c.T.Helper()
	if _, ok := c.Brokers[id]; ok {
		c.T.Fatalf("broker %d already running", id)
	}
	var b *broker.Broker
	var err error
	// The port may linger briefly after close; retry for a moment.
	for i := 0; i < 50; i++ {
		b, err = broker.New(c.cfgs[id])
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		c.T.Fatalf("restart broker %d: %v", id, err)
	}
	c.Brokers[id] = b
	return b
}

// Isolate cuts every link between broker id and everyone else (other
// brokers and clients).
func (c *Cluster) Isolate(id int32) {
	for other := range c.cfgs {
		if other != id {
			c.Faults.Block(c.Addr(id), c.Addr(other))
		}
	}
	c.Faults.Block(c.Addr(id), "client")
}

// Heal restores all links.
func (c *Cluster) Heal() { c.Faults.HealAll() }

// Close stops every broker.
func (c *Cluster) Close() {
	for id := range c.Brokers {
		c.Stop(id)
	}
}

// Eventually polls cond until it returns nil or the timeout expires.
func Eventually(t testing.TB, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for time.Now().Before(deadline) {
		if err = cond(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %v", timeout, err)
}

// Ctx returns a context with timeout, cancelled on cleanup.
func Ctx(t testing.TB, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
