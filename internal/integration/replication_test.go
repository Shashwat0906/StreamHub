package integration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

func minISR2(c *broker.Config) { c.MinInsyncReplicas = 2 }

// replicaLog returns the full log of a broker's replica.
func replicaLog(t *testing.T, b *broker.Broker, topic string, part int32) []storage.Record {
	t.Helper()
	p := b.Partition(topic, part)
	if p == nil {
		return nil
	}
	recs, err := p.ReadAll()
	if err != nil {
		t.Fatalf("broker %d read: %v", b.ID(), err)
	}
	return recs
}

func partitionMeta(cl *testutil.Cluster, topic string, part int32) metadata.PartitionMeta {
	for _, b := range cl.Brokers {
		if b.IsController() {
			pm, _ := b.Metadata().Partition(topic, part)
			return pm
		}
	}
	for _, b := range cl.Brokers {
		pm, _ := b.Metadata().Partition(topic, part)
		return pm
	}
	return metadata.PartitionMeta{}
}

// waitReplicasIdentical waits until every running replica of the
// partition holds exactly the same records (offset, epoch, value).
func waitReplicasIdentical(t *testing.T, cl *testutil.Cluster, topic string, part int32, timeout time.Duration) []storage.Record {
	t.Helper()
	var ref []storage.Record
	testutil.Eventually(t, timeout, func() error {
		pm := partitionMeta(cl, topic, part)
		ref = nil
		for _, id := range pm.Replicas {
			b, ok := cl.Brokers[id]
			if !ok {
				continue
			}
			recs := replicaLog(t, b, topic, part)
			if ref == nil {
				ref = recs
				continue
			}
			if len(recs) != len(ref) {
				return fmt.Errorf("broker %d has %d records, another replica %d", id, len(recs), len(ref))
			}
			for i := range recs {
				if recs[i].Offset != ref[i].Offset || recs[i].LeaderEpoch != ref[i].LeaderEpoch || string(recs[i].Value) != string(ref[i].Value) {
					return fmt.Errorf("broker %d differs at index %d", id, i)
				}
			}
		}
		return nil
	})
	return ref
}

func TestReplicationAcksAllAllReplicasIdentical(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "rep", Partitions: 2, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for i := 0; i < 500; i++ {
		wg.Add(1)
		p.Send(ctx, client.Message{Topic: "rep", Key: []byte(strconv.Itoa(i % 7)), Value: []byte(strconv.Itoa(i))}, func(d client.Delivery) {
			if d.Err != nil {
				mu.Lock()
				errs = append(errs, d.Err)
				mu.Unlock()
			}
			wg.Done()
		})
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("%d deliveries failed, first: %v", len(errs), errs[0])
	}
	total := 0
	for part := int32(0); part < 2; part++ {
		recs := waitReplicasIdentical(t, cl, "rep", part, 10*time.Second)
		total += len(recs)
		// HW reaches the end on the leader once all ISR members fetched.
		pm := partitionMeta(cl, "rep", part)
		if len(pm.ISR) != 3 {
			t.Fatalf("partition %d ISR %v, want all 3", part, pm.ISR)
		}
	}
	if total != 500 {
		t.Fatalf("replicated %d records, want 500", total)
	}
}

func TestISRShrinksAndExpands(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "isr", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	pm := partitionMeta(cl, "isr", 0)
	ctrl := cl.Controller()
	// Stop a follower that is neither the leader nor the controller.
	var victim int32 = -1
	for _, r := range pm.Replicas {
		if r != pm.Leader && r != ctrl {
			victim = r
		}
	}
	if victim < 0 {
		t.Skip("no suitable follower")
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	produce := func(n int, tag string) {
		for i := 0; i < n; i++ {
			if d := p.SendSync(ctx, client.Message{Topic: "isr", Value: []byte(fmt.Sprintf("%s-%d", tag, i))}); d.Err != nil {
				t.Fatalf("%s %d: %v", tag, i, d.Err)
			}
		}
	}
	produce(20, "before")
	cl.Kill(victim)
	testutil.Eventually(t, 10*time.Second, func() error {
		if isr := partitionMeta(cl, "isr", 0).ISR; slices.Contains(isr, victim) {
			return fmt.Errorf("isr %v still has %d", isr, victim)
		}
		return nil
	})
	// acks=all keeps working with 2 of 3 in sync (min.insync.replicas=2).
	produce(50, "during")
	cl.Start(victim)
	// The restarted follower catches up and rejoins the ISR.
	testutil.Eventually(t, 15*time.Second, func() error {
		if isr := partitionMeta(cl, "isr", 0).ISR; len(isr) != 3 {
			return fmt.Errorf("isr %v", isr)
		}
		return nil
	})
	recs := waitReplicasIdentical(t, cl, "isr", 0, 10*time.Second)
	if len(recs) != 70 {
		t.Fatalf("got %d records, want 70", len(recs))
	}
}

func TestNotEnoughReplicas(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "nem", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatal(err)
	}
	pm := partitionMeta(cl, "nem", 0)
	ctrl := cl.Controller()
	var follower int32 = -1
	for _, r := range pm.Replicas {
		if r != pm.Leader {
			follower = r
		}
	}
	if follower == ctrl {
		t.Skip("follower is the controller; scenario needs a quorum to stay")
	}
	cl.Kill(follower)
	testutil.Eventually(t, 10*time.Second, func() error {
		if isr := partitionMeta(cl, "nem", 0).ISR; len(isr) != 1 {
			return fmt.Errorf("isr %v", isr)
		}
		return nil
	})
	// acks=all is refused: only 1 in-sync replica < min.insync.replicas=2.
	pAll, _ := c.NewProducer(client.ProducerConfig{DeliveryTimeout: 2 * time.Second})
	defer pAll.Close()
	d := pAll.SendSync(ctx, client.Message{Topic: "nem", Value: []byte("x")})
	var pe *protocol.Error
	// Either code is correct: NOT_ENOUGH_REPLICAS if the leader already saw
	// the shrunk ISR before appending, ..._AFTER_APPEND if the ISR shrank
	// while the write waited for the high-watermark.
	if !errors.As(d.Err, &pe) || (pe.Code != protocol.ErrNotEnoughReplicas && pe.Code != protocol.ErrNotEnoughReplicasAfter) {
		t.Fatalf("acks=all with ISR=1: got %v, want NOT_ENOUGH_REPLICAS(_AFTER_APPEND)", d.Err)
	}
	// acks=1 is accepted (and is exactly the mode that can lose data).
	p1, _ := c.NewProducer(client.ProducerConfig{Acks: protocol.AcksLeader, AcksSet: true})
	defer p1.Close()
	if d := p1.SendSync(ctx, client.Message{Topic: "nem", Value: []byte("y")}); d.Err != nil {
		t.Fatalf("acks=1: %v", d.Err)
	}
}

// The core durability test: kill the partition leader while producers are
// sending with acks=all, then verify every acknowledged record survived.
func TestFailover_KillLeaderMidProduce(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 90*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "fo", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true, Linger: time.Millisecond, DeliveryTimeout: 30 * time.Second})
	defer p.Close()

	var mu sync.Mutex
	acked := map[int]int64{} // value -> offset
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			d := p.SendSync(ctx, client.Message{Topic: "fo", Value: []byte(strconv.Itoa(i))})
			if d.Err == nil {
				mu.Lock()
				acked[i] = d.Offset
				mu.Unlock()
			}
		}
	}()

	waitAcked := func(n int) {
		testutil.Eventually(t, 30*time.Second, func() error {
			mu.Lock()
			defer mu.Unlock()
			if len(acked) < n {
				return fmt.Errorf("only %d acked", len(acked))
			}
			return nil
		})
	}
	waitAcked(200)
	oldLeader := partitionMeta(cl, "fo", 0).Leader
	t.Logf("killing leader %d after %d acks", oldLeader, len(acked))
	cl.Kill(oldLeader)
	mu.Lock()
	ackedAtKill := len(acked)
	mu.Unlock()
	waitAcked(ackedAtKill + 200) // progress resumes on the new leader
	close(stop)
	wg.Wait()

	newLeader := partitionMeta(cl, "fo", 0).Leader
	if newLeader == oldLeader || newLeader < 0 {
		t.Fatalf("no failover: leader %d", newLeader)
	}
	recs := waitReplicasIdentical(t, cl, "fo", 0, 10*time.Second)
	seen := map[int]int{}
	for _, r := range recs {
		v, _ := strconv.Atoi(string(r.Value))
		seen[v]++
	}
	lost, dups := 0, 0
	for v, off := range acked {
		if seen[v] == 0 {
			lost++
			t.Errorf("acknowledged value %d (offset %d) lost", v, off)
		}
	}
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	t.Logf("acked=%d stored=%d lost=%d duplicates=%d old_leader=%d new_leader=%d",
		len(acked), len(recs), lost, dups, oldLeader, newLeader)
	if lost > 0 {
		t.Fatalf("%d acknowledged records lost", lost)
	}
	if dups > 0 {
		t.Fatalf("idempotent producer wrote %d duplicates", dups)
	}
	// The old leader comes back as a follower and converges.
	cl.Start(oldLeader)
	waitReplicasIdentical(t, cl, "fo", 0, 20*time.Second)
}

// Cut the leader off from the other brokers (clients can still reach it).
// It must stop acknowledging acks=all writes, a new leader is elected on
// the majority side, and after healing the old leader truncates any
// divergent (unacknowledged-by-ISR) tail so all replicas are identical.
func TestNetworkPartitionIsolatedLeader(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 90*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "np", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	pAll, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer pAll.Close()
	ackedAll := map[string]bool{}
	for i := 0; i < 50; i++ {
		v := fmt.Sprintf("all-%d", i)
		if d := pAll.SendSync(ctx, client.Message{Topic: "np", Value: []byte(v)}); d.Err != nil {
			t.Fatal(d.Err)
		}
		ackedAll[v] = true
	}
	old := partitionMeta(cl, "np", 0).Leader
	oldAddr := cl.Addr(old)
	for _, id := range cl.IDs() {
		if id != old {
			cl.Faults.Block(oldAddr, cl.Addr(id))
		}
	}

	// Write directly to the isolated leader with acks=1: some of these may
	// be accepted before it self-fences; they are NOT replicated.
	directAcks1 := directProduce(t, oldAddr, "np", protocol.AcksLeader, 20)
	// acks=all to the isolated leader must never succeed.
	if n := directProduce(t, oldAddr, "np", protocol.AcksAll, 3); n > 0 {
		t.Fatalf("isolated leader acknowledged %d acks=all writes", n)
	}
	t.Logf("isolated leader accepted %d acks=1 writes", directAcks1)

	// Majority side elects a new leader.
	testutil.Eventually(t, 15*time.Second, func() error {
		for _, id := range cl.IDs() {
			if id == old {
				continue
			}
			pm, _ := cl.Brokers[id].Metadata().Partition("np", 0)
			if pm.Leader != old && pm.Leader >= 0 {
				return nil
			}
		}
		return fmt.Errorf("no new leader yet")
	})
	for i := 50; i < 100; i++ {
		v := fmt.Sprintf("all-%d", i)
		if d := pAll.SendSync(ctx, client.Message{Topic: "np", Value: []byte(v)}); d.Err != nil {
			t.Fatal(d.Err)
		}
		ackedAll[v] = true
	}

	cl.Heal()
	recs := waitReplicasIdentical(t, cl, "np", 0, 30*time.Second)
	have := map[string]bool{}
	for _, r := range recs {
		have[string(r.Value)] = true
	}
	for v := range ackedAll {
		if !have[v] {
			t.Fatalf("acks=all record %s lost", v)
		}
	}
	survivedAcks1 := 0
	for v := range have {
		if len(v) > 6 && v[:6] == "direct" {
			survivedAcks1++
		}
	}
	t.Logf("acks=1 writes to the isolated leader: accepted=%d survived=%d (acks=1 gives no durability guarantee)", directAcks1, survivedAcks1)
}

// directProduce sends n single-record batches straight to addr (bypassing
// client routing) and returns how many were acknowledged.
func directProduce(t *testing.T, addr, topic string, acks int16, n int) int {
	t.Helper()
	cl, _ := client.New(client.Config{Bootstrap: []string{addr}, RequestTimeout: 2 * time.Second})
	defer cl.Close()
	ok := 0
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := cl.RawProduce(ctx, addr, &protocol.ProduceRequest{
			Acks: acks, TimeoutMs: 1000, Topic: topic, Partition: 0, ProducerID: -1, BaseSequence: -1,
			Records: []storage.Record{{Value: []byte(fmt.Sprintf("direct-%d-%d", acks, i))}},
		})
		cancel()
		if err == nil {
			ok++
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ok
}

func TestBrokerRestartRejoinsISR(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "rs", Partitions: 3, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	for i := 0; i < 300; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "rs", Value: []byte(strconv.Itoa(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	// Restart every broker one by one (rolling restart).
	for _, id := range []int32{1, 2, 3} {
		cl.Stop(id)
		for i := 0; i < 30; i++ {
			if d := p.SendSync(ctx, client.Message{Topic: "rs", Value: []byte(fmt.Sprintf("r%d-%d", id, i))}); d.Err != nil {
				t.Fatalf("produce while broker %d down: %v", id, d.Err)
			}
		}
		cl.Start(id)
		cl.WaitReady(3)
		testutil.Eventually(t, 20*time.Second, func() error {
			for part := int32(0); part < 3; part++ {
				if isr := partitionMeta(cl, "rs", part).ISR; len(isr) != 3 {
					return fmt.Errorf("partition %d isr %v", part, isr)
				}
			}
			return nil
		})
	}
	total := 0
	for part := int32(0); part < 3; part++ {
		total += len(waitReplicasIdentical(t, cl, "rs", part, 10*time.Second))
	}
	if total != 390 {
		t.Fatalf("total records %d, want 390", total)
	}
}

// Simulates a crash in the middle of a disk write on a follower: the
// broker is stopped, half a record is appended to its active segment, and
// on restart recovery must cut the torn tail and re-replicate the rest.
func TestCrashDuringWriteRecovers(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "torn", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	for i := 0; i < 100; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "torn", Value: []byte(strconv.Itoa(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	waitReplicasIdentical(t, cl, "torn", 0, 10*time.Second)
	pm := partitionMeta(cl, "torn", 0)
	var victim int32 = -1
	for _, r := range pm.Replicas {
		if r != pm.Leader {
			victim = r
			break
		}
	}
	dir := cl.Brokers[victim].Partition("torn", 0).Dir()
	cl.Stop(victim)
	if err := testutil.AppendGarbage(dir, 37); err != nil {
		t.Fatal(err)
	}
	cl.Start(victim)
	cl.WaitReady(3)
	for i := 100; i < 150; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "torn", Value: []byte(strconv.Itoa(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	recs := waitReplicasIdentical(t, cl, "torn", 0, 15*time.Second)
	if len(recs) != 150 {
		t.Fatalf("got %d records, want 150", len(recs))
	}
	for i, r := range recs {
		if string(r.Value) != strconv.Itoa(i) {
			t.Fatalf("record %d = %q", i, r.Value)
		}
	}
}
