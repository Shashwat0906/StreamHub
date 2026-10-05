package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

// ---------------------------------------------------------------- idempotence

func batch(pid int64, seq int32, vals ...string) *protocol.ProduceRequest {
	req := &protocol.ProduceRequest{Acks: protocol.AcksAll, TimeoutMs: 5000, Topic: "idem", ProducerID: pid, BaseSequence: seq}
	for _, v := range vals {
		req.Records = append(req.Records, storage.Record{Value: []byte(v)})
	}
	return req
}

func rawProduce(t *testing.T, c *client.Client, addr string, req *protocol.ProduceRequest) (int64, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.RawProduceOffset(ctx, addr, req)
}

func leaderAddr(t *testing.T, cl *testutil.Cluster, topic string, part int32) string {
	t.Helper()
	var addr string
	testutil.Eventually(t, 10*time.Second, func() error {
		pm := partitionMeta(cl, topic, part)
		if pm.Leader < 0 {
			return fmt.Errorf("no leader")
		}
		addr = cl.Addr(pm.Leader)
		return nil
	})
	return addr
}

func TestIdempotentRetryIsDeduplicated(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "idem", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	addr := leaderAddr(t, cl, "idem", 0)
	const pid = 424242

	off, err := rawProduce(t, c, addr, batch(pid, 0, "a", "b", "c"))
	if err != nil || off != 0 {
		t.Fatalf("first batch: %d %v", off, err)
	}
	// A retry of the same batch (e.g. the response was lost) is not
	// written again and reports the original offset.
	off, err = rawProduce(t, c, addr, batch(pid, 0, "a", "b", "c"))
	if err != nil || off != 0 {
		t.Fatalf("retried batch: %d %v", off, err)
	}
	off, err = rawProduce(t, c, addr, batch(pid, 3, "d"))
	if err != nil || off != 3 {
		t.Fatalf("next batch: %d %v", off, err)
	}
	// A gap in sequence numbers means a lost batch: rejected.
	_, err = rawProduce(t, c, addr, batch(pid, 10, "x"))
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != protocol.ErrOutOfOrderSequence {
		t.Fatalf("sequence gap: got %v, want OUT_OF_ORDER_SEQUENCE", err)
	}
	// Without a producer ID the same "retry" is written twice: this is the
	// at-least-once behaviour idempotence removes.
	rawProduce(t, c, addr, batch(-1, -1, "dup"))
	rawProduce(t, c, addr, batch(-1, -1, "dup"))

	recs := waitReplicasIdentical(t, cl, "idem", 0, 10*time.Second)
	var vals []string
	for _, r := range recs {
		vals = append(vals, string(r.Value))
	}
	if got := strings.Join(vals, ","); got != "a,b,c,d,dup,dup" {
		t.Fatalf("log content %q", got)
	}
}

// Idempotence state lives in the records (producerId, sequence), so a
// follower that becomes leader de-duplicates a retry of a batch that the
// old leader wrote.
func TestIdempotenceSurvivesLeaderFailover(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "idem", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	old := partitionMeta(cl, "idem", 0).Leader
	if old == cl.Controller() {
		// Killing the controller too would also work but takes longer.
		t.Logf("leader %d is also controller", old)
	}
	const pid = 777
	if off, err := rawProduce(t, c, cl.Addr(old), batch(pid, 0, "p0", "p1")); err != nil || off != 0 {
		t.Fatalf("produce: %d %v", off, err)
	}
	waitReplicasIdentical(t, cl, "idem", 0, 10*time.Second)
	cl.Kill(old)
	var addr string
	testutil.Eventually(t, 15*time.Second, func() error {
		for _, id := range cl.IDs() {
			pm, _ := cl.Brokers[id].Metadata().Partition("idem", 0)
			if pm.Leader >= 0 && pm.Leader != old {
				addr = cl.Addr(pm.Leader)
				return nil
			}
		}
		return fmt.Errorf("no new leader")
	})
	// The producer did not get the ack and retries against the new leader.
	var off int64
	var err error
	testutil.Eventually(t, 10*time.Second, func() error {
		off, err = rawProduce(t, c, addr, batch(pid, 0, "p0", "p1"))
		return err
	})
	if off != 0 {
		t.Fatalf("retry on new leader returned offset %d, want original 0", off)
	}
	recs := waitReplicasIdentical(t, cl, "idem", 0, 10*time.Second)
	if len(recs) != 2 {
		t.Fatalf("expected 2 records (no duplicate), have %d", len(recs))
	}
}

func TestIdempotenceSurvivesRestart(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "idem", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	rawProduce(t, c, cl.Addr(1), batch(55, 0, "one"))
	cl.Stop(1)
	cl.Start(1)
	c2 := cl.Client()
	var off int64
	testutil.Eventually(t, 10*time.Second, func() error {
		var err error
		off, err = rawProduce(t, c2, cl.Addr(1), batch(55, 0, "one"))
		return err
	})
	if off != 0 {
		t.Fatalf("duplicate after restart got offset %d", off)
	}
	if end, _ := c2.ListOffsets(ctx, "idem", 0, protocol.OffsetLatest); end != 1 {
		t.Fatalf("log end %d, want 1", end)
	}
}

// ---------------------------------------------------------------- retention

func TestRetentionBySizeOnAllReplicas(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	err := c.CreateTopic(ctx, client.TopicSpec{Name: "ret", Partitions: 1, ReplicationFactor: 3,
		Configs: map[string]string{"segment.bytes": "4096", "retention.bytes": "16384"}})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	val := strings.Repeat("x", 100)
	for i := 0; i < 1000; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "ret", Value: []byte(val)}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
	testutil.Eventually(t, 15*time.Second, func() error {
		for _, b := range cl.Brokers {
			part := b.Partition("ret", 0)
			if part == nil {
				continue
			}
			if size := part.LogSize(); size > 16384+4096+512 {
				return fmt.Errorf("broker %d log still %d bytes", b.ID(), size)
			}
			if part.LogStartOffset() == 0 {
				return fmt.Errorf("broker %d log start still 0", b.ID())
			}
		}
		return nil
	})
	earliest, err := c.ListOffsets(ctx, "ret", 0, protocol.OffsetEarliest)
	if err != nil || earliest == 0 {
		t.Fatalf("earliest %d %v", earliest, err)
	}
	// A consumer asking for a deleted offset is reset to the earliest one.
	pc, _ := c.ConsumePartition(ctx, "ret", 0, 0, client.FetchConfig{Reset: client.ResetEarliest})
	var recs []client.Record
	testutil.Eventually(t, 5*time.Second, func() error {
		var err error
		recs, err = pc.Poll(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return fmt.Errorf("no records")
		}
		return nil
	})
	if recs[0].Offset != earliest {
		t.Fatalf("reset to %d, earliest is %d", recs[0].Offset, earliest)
	}
	t.Logf("retention moved log start to %d of 1000", earliest)
}

func TestRetentionByTime(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	err := c.CreateTopic(ctx, client.TopicSpec{Name: "old", Partitions: 1,
		Configs: map[string]string{"segment.bytes": "2048", "retention.ms": "60000"}})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	past := time.Now().Add(-2 * time.Hour)
	for i := 0; i < 200; i++ {
		p.SendSync(ctx, client.Message{Topic: "old", Value: []byte(strings.Repeat("o", 50)), Timestamp: past})
	}
	for i := 0; i < 5; i++ {
		p.SendSync(ctx, client.Message{Topic: "old", Value: []byte("fresh")})
	}
	testutil.Eventually(t, 10*time.Second, func() error {
		start := cl.Brokers[1].Partition("old", 0).LogStartOffset()
		if start < 150 {
			return fmt.Errorf("log start %d", start)
		}
		return nil
	})
	// The newest (active) segment always stays.
	if end, _ := c.ListOffsets(ctx, "old", 0, protocol.OffsetLatest); end != 205 {
		t.Fatalf("end %d", end)
	}
}

// A follower that was down while the leader deleted old segments cannot
// fetch from its old position; it must restart from the leader's log start.
func TestFollowerResetsBehindLeaderLogStart(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2)
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	err := c.CreateTopic(ctx, client.TopicSpec{Name: "lag", Partitions: 1, ReplicationFactor: 3,
		Configs: map[string]string{"segment.bytes": "4096", "retention.bytes": "8192"}})
	if err != nil {
		t.Fatal(err)
	}
	pm := partitionMeta(cl, "lag", 0)
	ctrl := cl.Controller()
	var victim int32 = -1
	for _, r := range pm.Replicas {
		if r != pm.Leader && r != ctrl {
			victim = r
		}
	}
	if victim < 0 {
		t.Skip("no suitable follower")
	}
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	send := func(n int) {
		for i := 0; i < n; i++ {
			if d := p.SendSync(ctx, client.Message{Topic: "lag", Value: []byte(strings.Repeat("y", 100))}); d.Err != nil {
				t.Fatal(d.Err)
			}
		}
	}
	send(20)
	cl.Stop(victim)
	send(800)
	testutil.Eventually(t, 10*time.Second, func() error {
		if s := cl.Brokers[pm.Leader].Partition("lag", 0).LogStartOffset(); s <= 20 {
			return fmt.Errorf("leader log start %d", s)
		}
		return nil
	})
	cl.Start(victim)
	cl.WaitReady(3)
	testutil.Eventually(t, 15*time.Second, func() error {
		f := cl.Brokers[victim].Partition("lag", 0)
		l := cl.Brokers[pm.Leader].Partition("lag", 0)
		if f.LogEndOffset() != l.LogEndOffset() || f.LogEndOffset() != 820 {
			return fmt.Errorf("follower %d leader %d", f.LogEndOffset(), l.LogEndOffset())
		}
		return nil
	})
}

// ---------------------------------------------------------------- hardening

func TestMessageTooLarge(t *testing.T) {
	cl := testutil.NewCluster(t, 1)
	c := cl.Client()
	ctx := testutil.Ctx(t, 30*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "big", Partitions: 1, Configs: map[string]string{"max.message.bytes": "1000"}}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	d := p.SendSync(ctx, client.Message{Topic: "big", Value: make([]byte, 1001)})
	var pe *protocol.Error
	if !errors.As(d.Err, &pe) || pe.Code != protocol.ErrMessageTooLarge {
		t.Fatalf("got %v, want MESSAGE_TOO_LARGE", d.Err)
	}
	if d := p.SendSync(ctx, client.Message{Topic: "big", Value: make([]byte, 1000)}); d.Err != nil {
		t.Fatal(d.Err)
	}
}

func TestPreferredLeaderRestoredAfterRestart(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2, func(c *broker.Config) { c.PreferredLeaderInterval = 500 * time.Millisecond })
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "pref", Partitions: 3, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	ctrl := cl.Controller()
	var part int32 = -1
	var pref int32
	for i := int32(0); i < 3; i++ {
		pm := partitionMeta(cl, "pref", i)
		if pm.Replicas[0] != ctrl {
			part, pref = i, pm.Replicas[0]
			break
		}
	}
	cl.Kill(pref)
	testutil.Eventually(t, 10*time.Second, func() error {
		if l := partitionMeta(cl, "pref", part).Leader; l == pref || l < 0 {
			return fmt.Errorf("leader %d", l)
		}
		return nil
	})
	cl.Start(pref)
	testutil.Eventually(t, 20*time.Second, func() error {
		if l := partitionMeta(cl, "pref", part).Leader; l != pref {
			return fmt.Errorf("leader %d, preferred %d", l, pref)
		}
		return nil
	})
}

// ---------------------------------------------------------------- observability

func TestMetricsAndHealthEndpoints(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2, func(c *broker.Config) { c.HTTPAddr = "127.0.0.1:0" })
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "obs", Partitions: 2, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := c.NewProducer(client.ProducerConfig{})
	defer p.Close()
	for i := 0; i < 50; i++ {
		p.SendSync(ctx, client.Message{Topic: "obs", Value: []byte("m")})
	}
	get := func(b *broker.Broker, path string) (int, string) {
		resp, err := http.Get("http://" + b.HTTPAddr() + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	var all string
	for _, b := range cl.Brokers {
		_, body := get(b, "/metrics")
		all += body
	}
	for _, want := range []string{
		`streamhub_requests_total{api="Produce",error="NONE"}`,
		`streamhub_request_duration_seconds_bucket{api="Fetch",le="+Inf"}`,
		`streamhub_partition_high_watermark{partition="0",topic="obs"}`,
		`streamhub_partition_isr_size{partition="1",topic="obs"} 3`,
		`streamhub_is_controller 1`,
		"# TYPE streamhub_records_produced_total counter",
	} {
		if !strings.Contains(all, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
	for _, b := range cl.Brokers {
		code, body := get(b, "/healthz")
		var h broker.Health
		if code != 200 || json.Unmarshal([]byte(body), &h) != nil || h.BrokerID != b.ID() || !h.Ready {
			t.Fatalf("healthz %d %s", code, body)
		}
	}
	// A broker cut off from everyone reports not-ready once it self-fences.
	var victim *broker.Broker
	for _, b := range cl.Brokers {
		if !b.IsController() {
			victim = b
			break
		}
	}
	cl.Isolate(victim.ID())
	testutil.Eventually(t, 10*time.Second, func() error {
		if code, body := get(victim, "/readyz"); code != http.StatusServiceUnavailable {
			return fmt.Errorf("readyz %d %s", code, body)
		}
		return nil
	})
	cl.Heal()
	testutil.Eventually(t, 10*time.Second, func() error {
		if code, _ := get(victim, "/readyz"); code != 200 {
			return fmt.Errorf("readyz %d", code)
		}
		return nil
	})
}

// A graceful stop hands leadership over before the broker goes away, so
// clients see the new leader immediately instead of after the session
// timeout. (Compare with Kill in the failover tests.)
func TestControlledShutdownMovesLeadersFirst(t *testing.T) {
	cl := testutil.NewCluster(t, 3, minISR2, func(c *broker.Config) { c.SessionTimeout = 10 * time.Second })
	c := cl.Client()
	ctx := testutil.Ctx(t, 60*time.Second)
	if err := c.CreateTopic(ctx, client.TopicSpec{Name: "cs", Partitions: 3, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	ctrl := cl.Controller()
	var victim int32 = -1
	for i := int32(0); i < 3; i++ {
		if l := partitionMeta(cl, "cs", i).Leader; l != ctrl {
			victim = l
			break
		}
	}
	start := time.Now()
	cl.Stop(victim) // graceful
	// With a 10 s session timeout, only controlled shutdown can explain a
	// leader change within a couple of seconds.
	testutil.Eventually(t, 3*time.Second, func() error {
		for i := int32(0); i < 3; i++ {
			pm := partitionMeta(cl, "cs", i)
			if pm.Leader == victim {
				return fmt.Errorf("partition %d still led by %d", i, pm.Leader)
			}
		}
		return nil
	})
	t.Logf("leadership moved off broker %d within %v of a graceful stop (session timeout 10s)", victim, time.Since(start))
	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true})
	defer p.Close()
	for i := 0; i < 30; i++ {
		if d := p.SendSync(ctx, client.Message{Topic: "cs", Value: []byte("after")}); d.Err != nil {
			t.Fatal(d.Err)
		}
	}
}
