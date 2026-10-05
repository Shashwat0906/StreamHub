package integration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

func groupCfg(group string, topics ...string) client.GroupConfig {
	return client.GroupConfig{
		Group: group, Topics: topics,
		SessionTimeout: 1500 * time.Millisecond, HeartbeatInterval: 300 * time.Millisecond,
		RebalanceTimeout: 5 * time.Second,
		Fetch:            client.FetchConfig{MaxWait: 100 * time.Millisecond},
	}
}

func produceN(t *testing.T, c *client.Client, topic string, from, n int) {
	t.Helper()
	ctx := testutil.Ctx(t, 60*time.Second)
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	var wg sync.WaitGroup
	var failed error
	var mu sync.Mutex
	for i := from; i < from+n; i++ {
		wg.Add(1)
		p.Send(ctx, client.Message{Topic: topic, Key: []byte(strconv.Itoa(i)), Value: []byte(strconv.Itoa(i))}, func(d client.Delivery) {
			if d.Err != nil {
				mu.Lock()
				failed = d.Err
				mu.Unlock()
			}
			wg.Done()
		})
	}
	wg.Wait()
	if failed != nil {
		t.Fatal(failed)
	}
}

// member runs a group consumer in the background, committing after each
// batch, until stopped.
type member struct {
	gc     *client.GroupConsumer
	cl     *client.Client
	mu     sync.Mutex
	got    []client.Record
	stop   chan struct{}
	done   chan struct{}
	commit atomic.Bool
}

func startMember(t *testing.T, cl *testutil.Cluster, cfg client.GroupConfig) *member {
	c := cl.Client()
	gc, err := c.NewGroupConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m := &member{gc: gc, cl: c, stop: make(chan struct{}), done: make(chan struct{})}
	m.commit.Store(true)
	go func() {
		defer close(m.done)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-m.stop; cancel() }()
		for ctx.Err() == nil {
			recs, err := gc.Poll(ctx)
			if err != nil {
				continue
			}
			m.mu.Lock()
			m.got = append(m.got, recs...)
			m.mu.Unlock()
			if len(recs) > 0 && m.commit.Load() {
				gc.Commit(ctx)
			}
		}
	}()
	return m
}

func (m *member) records() []client.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]client.Record(nil), m.got...)
}

func (m *member) partitions() []string {
	var out []string
	for _, tp := range m.gc.Assignment() {
		for _, p := range tp.Partitions {
			out = append(out, fmt.Sprintf("%s-%d", tp.Topic, p))
		}
	}
	sort.Strings(out)
	return out
}

// leave stops the member gracefully (commit + LeaveGroup).
func (m *member) leave() {
	close(m.stop)
	<-m.done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.gc.Close(ctx)
}

// crash stops the member without committing or leaving: its connections
// are closed, heartbeats stop, and the coordinator must notice via the
// session timeout.
func (m *member) crash() {
	m.commit.Store(false)
	m.cl.Close()
	close(m.stop)
	<-m.done
}

func waitAssignments(t *testing.T, members []*member, total int) {
	t.Helper()
	testutil.Eventually(t, 15*time.Second, func() error {
		seen := map[string]bool{}
		gen := members[0].gc.Generation()
		for i, m := range members {
			if m.gc.Generation() != gen {
				return fmt.Errorf("generations differ")
			}
			for _, p := range m.partitions() {
				if seen[p] {
					return fmt.Errorf("%s assigned twice", p)
				}
				seen[p] = true
			}
			if len(m.partitions()) == 0 && total >= len(members) {
				return fmt.Errorf("member %d has no partitions", i)
			}
		}
		if len(seen) != total {
			return fmt.Errorf("%d of %d partitions assigned", len(seen), total)
		}
		return nil
	})
}

func TestGroupSplitsPartitionsAndRebalances(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "g1", Partitions: 6, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	a := startMember(t, cl, groupCfg("grp", "g1"))
	b := startMember(t, cl, groupCfg("grp", "g1"))
	waitAssignments(t, []*member{a, b}, 6)
	if len(a.partitions()) != 3 || len(b.partitions()) != 3 {
		t.Fatalf("uneven split: %v / %v", a.partitions(), b.partitions())
	}
	genBefore := a.gc.Generation()

	// A third member joins: rebalance to 2/2/2.
	d := startMember(t, cl, groupCfg("grp", "g1"))
	waitAssignments(t, []*member{a, b, d}, 6)
	for _, m := range []*member{a, b, d} {
		if len(m.partitions()) != 2 {
			t.Fatalf("expected 2 partitions each, got %v %v %v", a.partitions(), b.partitions(), d.partitions())
		}
	}
	if a.gc.Generation() <= genBefore {
		t.Fatal("generation did not increase on rebalance")
	}

	// One leaves gracefully: back to 3/3 without waiting for a session timeout.
	start := time.Now()
	d.leave()
	waitAssignments(t, []*member{a, b}, 6)
	t.Logf("rebalance after graceful leave took %v", time.Since(start))

	produceN(t, c, "g1", 0, 300)
	testutil.Eventually(t, 15*time.Second, func() error {
		n := len(a.records()) + len(b.records()) + len(d.records())
		if n < 300 {
			return fmt.Errorf("consumed %d/300", n)
		}
		return nil
	})
	a.leave()
	b.leave()

	info, err := c.DescribeGroup(ctx, "grp")
	if err != nil {
		t.Fatal(err)
	}
	var committed int64
	for _, o := range info.Offsets {
		committed += o.Committed
		if o.Lag != 0 {
			t.Fatalf("lag %d on %s-%d after consuming everything", o.Lag, o.Topic, o.Partition)
		}
	}
	if committed != 300 {
		t.Fatalf("sum of committed offsets %d, want 300", committed)
	}
}

// A member crashes (no commit, no leave) after processing records it never
// committed. After the session timeout its partitions move to the
// survivor, which must resume exactly at the last committed offsets: the
// uncommitted records are delivered again (at-least-once) and nothing is
// lost.
func TestGroupMemberDeathResumesFromCommitted(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "g2", Partitions: 4, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	produceN(t, c, "g2", 0, 400)
	a := startMember(t, cl, groupCfg("crashy", "g2"))
	b := startMember(t, cl, groupCfg("crashy", "g2"))
	waitAssignments(t, []*member{a, b}, 4)
	testutil.Eventually(t, 15*time.Second, func() error {
		if n := len(a.records()) + len(b.records()); n < 400 {
			return fmt.Errorf("consumed %d/400", n)
		}
		return nil
	})
	// Wait until the first 400 are committed.
	committedSum := func() (map[string]int64, int64) {
		info, err := c.DescribeGroup(ctx, "crashy")
		if err != nil {
			return nil, -1
		}
		m := map[string]int64{}
		var sum int64
		for _, o := range info.Offsets {
			m[fmt.Sprintf("%s-%d", o.Topic, o.Partition)] = o.Committed
			sum += o.Committed
		}
		return m, sum
	}
	testutil.Eventually(t, 10*time.Second, func() error {
		if _, sum := committedSum(); sum != 400 {
			return fmt.Errorf("committed %d", sum)
		}
		return nil
	})

	// a keeps consuming but stops committing, then crashes.
	a.commit.Store(false)
	aParts := a.partitions()
	aBefore := len(a.records())
	produceN(t, c, "g2", 400, 200)
	testutil.Eventually(t, 15*time.Second, func() error {
		if n := len(a.records()) + len(b.records()); n < 600 {
			return fmt.Errorf("consumed %d/600", n)
		}
		return nil
	})
	uncommittedByA := len(a.records()) - aBefore
	committed, csum := committedSum()
	t.Logf("committed offsets at crash (sum %d): %v", csum, committed)
	bBefore := len(b.records())
	a.crash()
	crashAt := time.Now()

	testutil.Eventually(t, 15*time.Second, func() error {
		if len(b.partitions()) != 4 {
			return fmt.Errorf("b has %v", b.partitions())
		}
		return nil
	})
	t.Logf("partitions %v moved to survivor %v after crash", aParts, time.Since(crashAt))

	// b re-reads a's partitions starting exactly at the committed offsets.
	testutil.Eventually(t, 15*time.Second, func() error {
		firstAfter := map[string]int64{}
		for _, r := range b.records()[bBefore:] {
			k := fmt.Sprintf("%s-%d", r.Topic, r.Partition)
			if _, ok := firstAfter[k]; !ok {
				firstAfter[k] = r.Offset
			}
		}
		reread := 0
		for _, r := range b.records()[bBefore:] {
			if slices.Contains(aParts, fmt.Sprintf("%s-%d", r.Topic, r.Partition)) {
				reread++
			}
		}
		if reread < uncommittedByA {
			return fmt.Errorf("survivor re-read %d of %d uncommitted records", reread, uncommittedByA)
		}
		for _, p := range aParts {
			got, ok := firstAfter[p]
			if !ok {
				continue // that partition had nothing uncommitted
			}
			if got != committed[p] {
				return fmt.Errorf("%s resumed at %d, committed was %d", p, got, committed[p])
			}
		}
		seen := map[string]int{}
		for _, r := range append(a.records(), b.records()...) {
			seen[string(r.Value)]++
		}
		if len(seen) < 600 {
			return fmt.Errorf("%d of 600 distinct values seen", len(seen))
		}
		return nil
	})
	seen := map[string]int{}
	for _, r := range append(a.records(), b.records()...) {
		seen[string(r.Value)]++
	}
	redelivered := 0
	for _, n := range seen {
		redelivered += n - 1
	}
	t.Logf("a processed %d records without committing; after its crash %d records were redelivered, 0 lost",
		uncommittedByA, redelivered)
	if uncommittedByA > 0 && redelivered < uncommittedByA {
		t.Fatalf("expected the %d uncommitted records to be redelivered, saw %d", uncommittedByA, redelivered)
	}
	b.leave()
}

// A consumer that was kicked out (its generation is stale) must not be
// able to overwrite offsets committed by the new owner.
func TestZombieCommitRejected(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "g3", Partitions: 2, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	produceN(t, c, "g3", 0, 50)
	a := startMember(t, cl, groupCfg("zombie", "g3"))
	waitAssignments(t, []*member{a}, 2)
	oldGen := a.gc.Generation()
	oldMember := a.gc.MemberID()
	b := startMember(t, cl, groupCfg("zombie", "g3"))
	waitAssignments(t, []*member{a, b}, 2)

	// Commit pretending to be a at its old generation.
	err := rawCommit(t, c, "zombie", oldMember, oldGen, "g3", 0, 0)
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != protocol.ErrIllegalGeneration {
		t.Fatalf("stale-generation commit: got %v, want ILLEGAL_GENERATION", err)
	}
	err = rawCommit(t, c, "zombie", "nobody", a.gc.Generation(), "g3", 0, 0)
	if !errors.As(err, &pe) || pe.Code != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown-member commit: got %v", err)
	}
	a.leave()
	b.leave()
}

func rawCommit(t *testing.T, c *client.Client, group, memberID string, gen int32, topic string, part int32, off int64) error {
	t.Helper()
	ctx := testutil.Ctx(t, 10*time.Second)
	info, err := c.FindCoordinator(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	return c.RawCommit(ctx, info, &protocol.OffsetCommitRequest{
		Group: group, MemberID: memberID, Generation: gen,
		Offsets: []protocol.CommitOffset{{Topic: topic, Partition: part, Offset: off}},
	})
}

func TestRoundRobinAcrossTopics(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	for _, name := range []string{"rr-a", "rr-b"} {
		if err := c.CreateTopic(ctx, client.TopicSpec{Name: name, Partitions: 3, ReplicationFactor: 3}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := groupCfg("rr", "rr-a", "rr-b")
	cfg.Strategy = "roundrobin"
	a := startMember(t, cl, cfg)
	b := startMember(t, cl, cfg)
	waitAssignments(t, []*member{a, b}, 6)
	if len(a.partitions()) != 3 || len(b.partitions()) != 3 {
		t.Fatalf("round-robin split: %v / %v", a.partitions(), b.partitions())
	}
	info, err := c.DescribeGroup(ctx, "rr")
	if err != nil || info.Strategy != "roundrobin" || len(info.Members) != 2 {
		t.Fatalf("describe: %+v %v", info, err)
	}
	a.leave()
	b.leave()
}

// Kill the broker coordinating the group. Members rediscover the new
// coordinator (the new leader of the group's __consumer_offsets
// partition), rejoin, and committed offsets survive because they are
// replicated records.
func TestCoordinatorFailover(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 90*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "g4", Partitions: 2, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	produceN(t, c, "g4", 0, 200)
	a := startMember(t, cl, groupCfg("failover", "g4"))
	testutil.Eventually(t, 15*time.Second, func() error {
		if n := len(a.records()); n < 200 {
			return fmt.Errorf("consumed %d", n)
		}
		return nil
	})
	// Make sure the commit for everything landed.
	testutil.Eventually(t, 10*time.Second, func() error {
		info, err := c.DescribeGroup(ctx, "failover")
		if err != nil {
			return err
		}
		var sum int64
		for _, o := range info.Offsets {
			sum += o.Committed
		}
		if sum != 200 {
			return fmt.Errorf("committed %d", sum)
		}
		return nil
	})
	coord, err := c.FindCoordinator(ctx, "failover")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("killing coordinator broker %d", coord.NodeID)
	cl.Kill(coord.NodeID)

	produceN(t, c, "g4", 200, 100)
	testutil.Eventually(t, 30*time.Second, func() error {
		seen := map[string]bool{}
		for _, r := range a.records() {
			seen[string(r.Value)] = true
		}
		if len(seen) < 300 {
			return fmt.Errorf("%d of 300 distinct", len(seen))
		}
		return nil
	})
	newCoord, err := c.FindCoordinator(ctx, "failover")
	if err != nil || newCoord.NodeID == coord.NodeID {
		t.Fatalf("coordinator did not move: %+v %v", newCoord, err)
	}
	// Offsets committed before the failover were not lost: the member did
	// not start over from 0 (that would deliver ~200 duplicates).
	dups := len(a.records()) - 300
	t.Logf("records delivered %d for 300 produced (%d duplicates) new coordinator %d", len(a.records()), dups, newCoord.NodeID)
	if dups >= 200 {
		t.Fatalf("consumer restarted from scratch: committed offsets lost")
	}
	a.leave()
}
