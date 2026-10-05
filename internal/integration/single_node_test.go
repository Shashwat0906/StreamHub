// Package integration contains end-to-end tests that run real brokers
// (in-process, real TCP, real disk) and drive them through the client.
package integration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

func consumeAll(t *testing.T, c *client.Client, topic string, partition int32, want int) []client.Record {
	t.Helper()
	ctx := testutil.Ctx(t, 20*time.Second)
	pc, err := c.ConsumePartition(ctx, topic, partition, protocol.OffsetEarliest, client.FetchConfig{MaxWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var out []client.Record
	for len(out) < want {
		recs, err := pc.Poll(ctx)
		if err != nil {
			t.Fatalf("poll %s-%d after %d records: %v", topic, partition, len(out), err)
		}
		out = append(out, recs...)
	}
	return out
}

func TestSingleNodeProduceConsume(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "orders", Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	p, err := c.NewProducer(client.ProducerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	const n = 300
	placement := map[string]int32{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("customer-%d", i%10)
		d := p.SendSync(ctx, client.Message{Topic: "orders", Key: []byte(key), Value: []byte(fmt.Sprint(i))})
		if d.Err != nil {
			t.Fatal(d.Err)
		}
		// Same key => same partition, always.
		if prev, ok := placement[key]; ok && prev != d.Partition {
			t.Fatalf("key %s moved from partition %d to %d", key, prev, d.Partition)
		}
		placement[key] = d.Partition
	}

	total := 0
	for part := int32(0); part < 3; part++ {
		hw, err := c.ListOffsets(ctx, "orders", part, protocol.OffsetLatest)
		if err != nil {
			t.Fatal(err)
		}
		recs := consumeAll(t, c, "orders", part, int(hw))
		lastByKey := map[string]int{}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("partition %d: offset %d at position %d", part, r.Offset, i)
			}
			// Per-key order is preserved within the partition.
			var v int
			fmt.Sscan(string(r.Value), &v)
			if last, ok := lastByKey[string(r.Key)]; ok && v <= last {
				t.Fatalf("key %s out of order: %d after %d", r.Key, v, last)
			}
			lastByKey[string(r.Key)] = v
		}
		total += len(recs)
	}
	if total != n {
		t.Fatalf("consumed %d records, want %d", total, n)
	}
}

func TestAcksModes(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "acks", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	for _, acks := range []int16{protocol.AcksNone, protocol.AcksLeader, protocol.AcksAll} {
		p, err := c.NewProducer(client.ProducerConfig{Acks: acks, AcksSet: true})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			d := p.SendSync(ctx, client.Message{Topic: "acks", Value: []byte(fmt.Sprintf("acks%d-%d", acks, i))})
			if d.Err != nil {
				t.Fatalf("acks=%d: %v", acks, d.Err)
			}
			if acks != protocol.AcksNone && d.Offset < 0 {
				t.Fatalf("acks=%d: no offset returned", acks)
			}
		}
		p.Close()
	}
	// acks=0 is fire-and-forget; all 30 records still land on a healthy broker.
	testutil.Eventually(t, 5*time.Second, func() error {
		hw, err := c.ListOffsets(ctx, "acks", 0, protocol.OffsetLatest)
		if err != nil {
			return err
		}
		if hw != 30 {
			return fmt.Errorf("hw %d", hw)
		}
		return nil
	})
}

func TestLongPollWakesOnProduce(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "lp", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	pc, err := c.ConsumePartition(ctx, "lp", 0, 0, client.FetchConfig{MaxWait: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	go func() {
		time.Sleep(300 * time.Millisecond)
		p.SendSync(ctx, client.Message{Topic: "lp", Value: []byte("wake up")})
	}()
	start := time.Now()
	recs, err := pc.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if len(recs) != 1 || string(recs[0].Value) != "wake up" {
		t.Fatalf("got %+v", recs)
	}
	// The poll must return when data arrives, not when MaxWait expires.
	if elapsed > 5*time.Second || elapsed < 200*time.Millisecond {
		t.Fatalf("long poll returned after %v", elapsed)
	}
	// An empty poll waits about MaxWait and returns nothing.
	pc2, _ := c.ConsumePartition(ctx, "lp", 0, 1, client.FetchConfig{MaxWait: 300 * time.Millisecond})
	start = time.Now()
	recs, err = pc2.Poll(ctx)
	if err != nil || len(recs) != 0 {
		t.Fatalf("empty poll: %v %d", err, len(recs))
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("empty poll returned too early: %v", d)
	}
}

func TestRestartKeepsTopicsAndData(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "durable", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	for i := 0; i < 50; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "durable", Partition: 0, ManualPartition: true, Value: []byte(fmt.Sprint(i))}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	p.Close()
	cl.Stop(1)
	cl.Start(1)

	c2 := cl.Client()
	recs := consumeAll(t, c2, "durable", 0, 50)
	for i, r := range recs {
		if string(r.Value) != fmt.Sprint(i) {
			t.Fatalf("record %d = %q", i, r.Value)
		}
	}
	p2, _ := c2.NewProducer(client.ProducerConfig{})
	defer p2.Close()
	d := p2.SendSync(ctx, client.Message{Topic: "durable", Partition: 0, ManualPartition: true, Value: []byte("after restart")})
	if d.Err != nil || d.Offset != 50 {
		t.Fatalf("produce after restart: offset %d err %v", d.Offset, d.Err)
	}
}

func TestCreateTopicValidation(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "dup", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		spec client.TopicSpec
		code protocol.ErrorCode
	}{
		{client.TopicSpec{Name: "dup", Partitions: 1}, protocol.ErrTopicAlreadyExists},
		{client.TopicSpec{Name: "bad name!", Partitions: 1}, protocol.ErrInvalidTopic},
		{client.TopicSpec{Name: "rf", Partitions: 1, ReplicationFactor: 3}, protocol.ErrInvalidReplicationFactor},
		{client.TopicSpec{Name: "cfg", Partitions: 1, Configs: map[string]string{"nope": "1"}}, protocol.ErrInvalidRequest},
	}
	for _, tc := range cases {
		err := c.CreateTopic(ctx, tc.spec)
		var pe *protocol.Error
		if !errors.As(err, &pe) || pe.Code != tc.code {
			t.Fatalf("%+v: got %v, want %s", tc.spec, err, tc.code)
		}
	}
}

func TestDeleteTopicRemovesData(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "gone", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	p.SendSync(ctx, client.Message{Topic: "gone", Value: []byte("x")})
	p.Close()
	b := cl.Brokers[1]
	dir := b.Partition("gone", 0).Dir()
	if err := c.DeleteTopic(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 5*time.Second, func() error {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			return fmt.Errorf("log dir %s still exists", filepath.Base(dir))
		}
		return nil
	})
	short := testutil.Ctx(t, 2*time.Second)
	if _, err := c.ListOffsets(short, "gone", 0, protocol.OffsetLatest); err == nil {
		t.Fatal("expected error for deleted topic")
	}
}
