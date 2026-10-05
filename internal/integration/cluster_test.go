package integration

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

func TestClusterFormsAndSpreadsPartitions(t *testing.T) {
	cl := testutil.NewCluster(t, 3)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)

	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "spread", Partitions: 6, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	// Metadata on followers is eventually consistent (it is replayed from
	// the Raft log), so poll until whichever broker we ask has the topic.
	testutil.Eventually(t, 5*time.Second, func() error {
		info, err := c.DescribeCluster(ctx)
		if err != nil {
			return err
		}
		if len(info.Brokers) != 3 || info.ControllerID < 1 {
			return fmt.Errorf("cluster info %+v", info)
		}
		leaders := map[int32]int{}
		for _, tpc := range info.Topics {
			for _, p := range tpc.Partitions {
				leaders[p.Leader]++
			}
		}
		if len(leaders) != 3 {
			return fmt.Errorf("partitions not spread over all brokers: %v", leaders)
		}
		return nil
	})

	// Every broker has the same metadata image (all replayed the same log).
	testutil.Eventually(t, 5*time.Second, func() error {
		ref := ""
		for id, b := range cl.Brokers {
			img := metadataImage(b.Metadata().Topics())
			if ref == "" {
				ref = img
				continue
			}
			if img != ref {
				return fmt.Errorf("broker %d metadata differs:\n%s\nvs\n%s", id, img, ref)
			}
		}
		return nil
	})

	// The client routes each partition to its leader.
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	for i := 0; i < 120; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "spread", Value: []byte(fmt.Sprint(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	total := 0
	for part := int32(0); part < 6; part++ {
		hw, err := c.ListOffsets(ctx, "spread", part, protocol.OffsetLatest)
		if err != nil {
			t.Fatal(err)
		}
		total += int(hw)
	}
	if total != 120 {
		t.Fatalf("records across partitions: %d", total)
	}
}

func TestControllerFailover(t *testing.T) {
	cl := testutil.NewCluster(t, 3)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	old := cl.Controller()
	cl.Kill(old)

	// A new controller is elected among the survivors...
	testutil.Eventually(t, 10*time.Second, func() error {
		for id, b := range cl.Brokers {
			if b.IsController() && id != old {
				return nil
			}
		}
		return fmt.Errorf("no new controller")
	})
	// ...it fences the dead broker...
	testutil.Eventually(t, 10*time.Second, func() error {
		for _, b := range cl.Brokers {
			if bm, _ := b.Metadata().Broker(old); !bm.Fenced {
				return fmt.Errorf("broker %d not fenced yet", old)
			}
		}
		return nil
	})
	// ...and metadata operations keep working.
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "after-failover", Partitions: 2, ReplicationFactor: 2}); err != nil {
		t.Fatal(err)
	}
	tm, _ := cl.Brokers[cl.Controller()].Metadata().Topic("after-failover")
	for _, p := range tm.Partitions {
		if slices.Contains(p.Replicas, old) {
			t.Fatalf("new topic placed on dead broker: %v", p.Replicas)
		}
	}

	// The old controller comes back as a normal member and is unfenced.
	cl.Start(old)
	cl.WaitReady(3)
}

func TestOfflinePartitionRecoversWhenBrokerReturns(t *testing.T) {
	cl := testutil.NewCluster(t, 3)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "solo", Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	ctrl := cl.Controller()
	// Pick a partition led by a non-controller broker and write to it.
	tm, _ := cl.Brokers[ctrl].Metadata().Topic("solo")
	var victim int32 = -1
	var part int32
	for _, p := range tm.Partitions {
		if p.Leader != ctrl {
			victim, part = p.Leader, p.ID
			break
		}
	}
	if victim < 0 {
		t.Skip("all partitions on controller")
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	for i := 0; i < 10; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "solo", Partition: part, ManualPartition: true, Value: []byte(fmt.Sprint(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	cl.Kill(victim)
	// RF=1: the only replica is gone, so the partition is offline (leader
	// -1). Nothing else can serve it without losing data.
	testutil.Eventually(t, 10*time.Second, func() error {
		pm, _ := cl.Brokers[ctrl].Metadata().Partition("solo", part)
		if pm.Leader != metadata.NoLeader {
			return fmt.Errorf("leader %d", pm.Leader)
		}
		return nil
	})
	cl.Start(victim)
	cl.WaitReady(3)
	testutil.Eventually(t, 10*time.Second, func() error {
		pm, _ := cl.Brokers[ctrl].Metadata().Partition("solo", part)
		if pm.Leader != victim {
			return fmt.Errorf("leader %d", pm.Leader)
		}
		return nil
	})
	recs := consumeAll(t, c, "solo", part, 10)
	if len(recs) != 10 {
		t.Fatalf("got %d records back", len(recs))
	}
}

func TestFullClusterRestartKeepsMetadata(t *testing.T) {
	cl := testutil.NewCluster(t, 3)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	for _, name := range []string{"a", "b"} {
		if err := c.CreateTopic(ctx, client.TopicSpec{Name: name, Partitions: 2, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range cl.IDs() {
		cl.Stop(id)
	}
	for id := int32(1); id <= 3; id++ {
		cl.Start(id)
	}
	cl.WaitReady(3)
	for _, b := range cl.Brokers {
		for _, name := range []string{"a", "b"} {
			if _, ok := b.Metadata().Topic(name); !ok {
				t.Fatalf("broker %d lost topic %s after restart", b.ID(), name)
			}
		}
	}
}

// metadataImage renders topic metadata by value for comparisons.
func metadataImage(ts []metadata.TopicMeta) string {
	out := ""
	for _, t := range ts {
		for _, p := range t.Partitions {
			out += fmt.Sprintf("%s-%d leader=%d epoch=%d replicas=%v isr=%v\n", t.Name, p.ID, p.Leader, p.LeaderEpoch, p.Replicas, p.ISR)
		}
	}
	return out
}
