package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// memNet routes RPCs between in-process nodes and can cut links.
type memNet struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[[2]string]bool
}

func (m *memNet) cut(a, b string) {
	m.mu.Lock()
	m.blocked[[2]string{a, b}] = true
	m.blocked[[2]string{b, a}] = true
	m.mu.Unlock()
}

func (m *memNet) heal() {
	m.mu.Lock()
	m.blocked = map[[2]string]bool{}
	m.mu.Unlock()
}

func (m *memNet) target(from, to string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked[[2]string{from, to}] {
		return nil, errors.New("link down")
	}
	n := m.nodes[to]
	if n == nil {
		return nil, errors.New("node down")
	}
	return n, nil
}

type memRPC struct {
	net  *memNet
	self string
}

func (r memRPC) Vote(ctx context.Context, addr string, req *protocol.RaftVoteRequest) (*protocol.RaftVoteResponse, error) {
	n, err := r.net.target(r.self, addr)
	if err != nil {
		return nil, err
	}
	return n.HandleVote(req), nil
}

func (r memRPC) Append(ctx context.Context, addr string, req *protocol.RaftAppendRequest) (*protocol.RaftAppendResponse, error) {
	n, err := r.net.target(r.self, addr)
	if err != nil {
		return nil, err
	}
	// Simulate the wire: decode copies, so nodes never share slices.
	b := protocol.Marshal(req)
	var cp protocol.RaftAppendRequest
	protocol.Unmarshal(b, &cp)
	return n.HandleAppend(&cp), nil
}

// listFSM records applied commands.
type listFSM struct {
	mu      sync.Mutex
	applied []string
}

func (f *listFSM) Apply(index uint64, data []byte) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(data))
	return len(f.applied)
}

func (f *listFSM) get() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...)
}

type testCluster struct {
	t     *testing.T
	net   *memNet
	peers map[int32]string
	dirs  map[int32]string
	nodes map[int32]*Node
	fsms  map[int32]*listFSM
}

func newTestCluster(t *testing.T, n int) *testCluster {
	tc := &testCluster{t: t, net: &memNet{nodes: map[string]*Node{}, blocked: map[[2]string]bool{}},
		peers: map[int32]string{}, dirs: map[int32]string{}, nodes: map[int32]*Node{}, fsms: map[int32]*listFSM{}}
	root := t.TempDir()
	for i := 1; i <= n; i++ {
		tc.peers[int32(i)] = fmt.Sprintf("n%d", i)
		tc.dirs[int32(i)] = filepath.Join(root, fmt.Sprintf("n%d", i))
	}
	for id := range tc.peers {
		tc.start(id)
	}
	t.Cleanup(func() {
		for id := range tc.nodes {
			tc.stop(id)
		}
	})
	return tc
}

func (tc *testCluster) start(id int32) {
	fsm := &listFSM{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	n, err := Start(Config{
		ID: id, Peers: tc.peers, Dir: tc.dirs[id],
		ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
		RPC: memRPC{net: tc.net, self: tc.peers[id]}, FSM: fsm, Logger: logger,
	})
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.net.mu.Lock()
	tc.net.nodes[tc.peers[id]] = n
	tc.net.mu.Unlock()
	tc.nodes[id] = n
	tc.fsms[id] = fsm
}

func (tc *testCluster) stop(id int32) {
	tc.net.mu.Lock()
	delete(tc.net.nodes, tc.peers[id])
	tc.net.mu.Unlock()
	tc.nodes[id].Close()
	delete(tc.nodes, id)
}

func (tc *testCluster) waitLeader(exclude ...int32) int32 {
	tc.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []int32
		for id, n := range tc.nodes {
			skip := false
			for _, e := range exclude {
				if e == id {
					skip = true
				}
			}
			if !skip && n.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatal("no single leader elected")
	return -1
}

func (tc *testCluster) propose(id int32, s string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := tc.nodes[id].Propose(ctx, []byte(s))
	return err
}

func (tc *testCluster) waitApplied(id int32, want []string) {
	tc.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := tc.fsms[id].get()
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("node %d applied %v, want %v", id, tc.fsms[id].get(), want)
}

func TestElectAndReplicate(t *testing.T) {
	tc := newTestCluster(t, 3)
	l := tc.waitLeader()
	for i := 0; i < 20; i++ {
		if err := tc.propose(l, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	var want []string
	for i := 0; i < 20; i++ {
		want = append(want, fmt.Sprint(i))
	}
	for id := range tc.nodes {
		tc.waitApplied(id, want)
	}
	// Followers reject proposals.
	for id, n := range tc.nodes {
		if id != l {
			if _, err := n.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrNotLeader) {
				t.Fatalf("follower accepted proposal: %v", err)
			}
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	l := tc.waitLeader()
	tc.propose(l, "a")
	tc.propose(l, "b")
	tc.stop(l)
	l2 := tc.waitLeader()
	if l2 == l {
		t.Fatal("dead node still leader")
	}
	if err := tc.propose(l2, "c"); err != nil {
		t.Fatal(err)
	}
	for id := range tc.nodes {
		tc.waitApplied(id, []string{"a", "b", "c"})
	}
	// The old leader restarts, recovers its log from disk and catches up.
	tc.start(l)
	tc.waitApplied(l, []string{"a", "b", "c"})
}

func TestMinorityCannotCommitAndLeaderStepsDown(t *testing.T) {
	tc := newTestCluster(t, 3)
	l := tc.waitLeader()
	tc.propose(l, "before")
	// Isolate the leader.
	for id, addr := range tc.peers {
		if id != l {
			tc.net.cut(tc.peers[l], addr)
		}
	}
	// The isolated leader cannot commit anything.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err := tc.nodes[l].Propose(ctx, []byte("lost"))
	cancel()
	if err == nil {
		t.Fatal("isolated leader committed an entry")
	}
	// The majority elects a new leader and keeps going.
	l2 := tc.waitLeader(l)
	if err := tc.propose(l2, "after"); err != nil {
		t.Fatal(err)
	}
	// Check-quorum: the old leader steps down by itself.
	deadline := time.Now().Add(3 * time.Second)
	for tc.nodes[l].IsLeader() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if tc.nodes[l].IsLeader() {
		t.Fatal("isolated leader never stepped down")
	}
	// Heal: the old leader's uncommitted "lost" entry is replaced.
	tc.net.heal()
	for id := range tc.nodes {
		tc.waitApplied(id, []string{"before", "after"})
	}
}

func TestSingleNode(t *testing.T) {
	tc := newTestCluster(t, 1)
	l := tc.waitLeader()
	if err := tc.propose(l, "solo"); err != nil {
		t.Fatal(err)
	}
	tc.waitApplied(l, []string{"solo"})
	tc.stop(l)
	tc.start(l)
	tc.waitLeader()
	tc.waitApplied(l, []string{"solo"}) // replayed from disk
}

func TestDiskLogRecoversTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := openDiskLog(dir)
	l.append(Entry{Term: 1, Index: 1, Data: []byte("a")}, Entry{Term: 1, Index: 2, Data: []byte("b")})
	l.close()
	f, _ := os.OpenFile(filepath.Join(dir, "log.wal"), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0, 0, 0, 40, 1, 2}) // half a record
	f.Close()
	l2, err := openDiskLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.close()
	if l2.lastIndex() != 2 || string(l2.entry(2).Data) != "b" {
		t.Fatalf("recovered %d entries", l2.lastIndex())
	}
	if err := l2.append(Entry{Term: 2, Index: 3, Data: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if err := l2.truncateFrom(2); err != nil || l2.lastIndex() != 1 {
		t.Fatalf("truncate: %v %d", err, l2.lastIndex())
	}
}
