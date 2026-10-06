package dashboard

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/testutil"
)

// startGateway runs the dashboard backend, attached to a real 3-broker
// in-process cluster, behind an httptest server.
func startGateway(t *testing.T) (*testutil.Cluster, *Gateway, string) {
	t.Helper()
	cl := testutil.NewCluster(t, 3, func(c *broker.Config) {
		c.HTTPAddr = "127.0.0.1:0"
		c.MinInsyncReplicas = 2
	})
	g, err := New(Config{Bootstrap: cl.Bootstrap(), Interval: 300 * time.Millisecond, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(g.Handler())
	t.Cleanup(func() { srv.Close(); g.Close() })
	testutil.Eventually(t, 15*time.Second, func() error {
		s := g.snapshot()
		if s == nil || !s.Reachable || s.Totals.HealthyBrokers != 3 {
			return fmt.Errorf("snapshot not ready: %+v", s)
		}
		return nil
	})
	return cl, g, srv.URL
}

func call(t *testing.T, method, url string, body any, out any) int {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func getSnapshot(t *testing.T, base string) Snapshot {
	var s Snapshot
	if code := call(t, "GET", base+"/api/snapshot", nil, &s); code != 200 {
		t.Fatalf("snapshot status %d", code)
	}
	return s
}

func findTopic(s Snapshot, name string) (TopicView, bool) {
	for _, tv := range s.Topics {
		if tv.Name == name {
			return tv, true
		}
	}
	return TopicView{}, false
}

// readSSE collects SSE events (name -> payloads) for d.
func readSSE(t *testing.T, url string, d time.Duration) map[string][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	out := map[string][]string{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	event := "message"
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			out[event] = append(out[event], strings.TrimPrefix(line, "data: "))
		case line == "":
			event = "message"
		}
	}
	return out
}

// End-to-end through the HTTP API: topic creation, produce with each acks
// mode, live data in the snapshot and over SSE, a demo consumer group, a
// failing message landing in the DLQ, retry (once only), and a broker
// failure visible as leader election + ISR shrink events.
func TestGatewayEndToEnd(t *testing.T) {
	cl, _, base := startGateway(t)

	// Snapshot basics: real brokers with HTTP state, controller known.
	s := getSnapshot(t, base)
	if s.Mode != "attached" || s.Capabilities.BrokerControl || len(s.Brokers) != 3 || s.ControllerID < 1 {
		t.Fatalf("snapshot: mode=%s caps=%+v brokers=%d controller=%d", s.Mode, s.Capabilities, len(s.Brokers), s.ControllerID)
	}
	for _, b := range s.Brokers {
		if !b.Reachable || b.HTTPAddr == "" || b.Status != "healthy" {
			t.Fatalf("broker %+v", b)
		}
	}

	if code := call(t, "POST", base+"/api/topics", CreateTopicRequest{Name: "payments", Partitions: 3, ReplicationFactor: 3}, nil); code/100 != 2 {
		t.Fatalf("create topic: %d", code)
	}

	// Produce with every acks mode.
	for _, acks := range []string{"all", "1", "0"} {
		var res ProduceResult
		code := call(t, "POST", base+"/api/produce", ProduceRequest{Topic: "payments", Key: "user-12", Value: `{"userId":12,"amount":500}`, Acks: acks}, &res)
		if code != 200 || res.Error != "" {
			t.Fatalf("produce acks=%s: %d %+v", acks, code, res)
		}
		if acks != "0" && (res.Offset < 0 || res.MessageID == "" || res.Status != "acknowledged") {
			t.Fatalf("produce acks=%s result %+v", acks, res)
		}
	}
	var bad ProduceResult
	if code := call(t, "POST", base+"/api/produce", ProduceRequest{Topic: "payments", Value: "x", Acks: "maybe"}, &bad); code != 400 {
		t.Fatalf("invalid acks accepted: %d", code)
	}

	// Snapshot reflects the writes (same key => same partition, 3 records).
	testutil.Eventually(t, 10*time.Second, func() error {
		tv, ok := findTopic(getSnapshot(t, base), "payments")
		if !ok || tv.Messages != 3 {
			return fmt.Errorf("payments: %+v", tv)
		}
		for _, p := range tv.Partitions {
			if len(p.ISR) != 3 || p.Leader < 0 {
				return fmt.Errorf("partition %+v", p)
			}
		}
		return nil
	})

	// SSE: /api/events sends a snapshot immediately and then periodically.
	evs := readSSE(t, base+"/api/events", 1500*time.Millisecond)
	if len(evs["snapshot"]) < 2 || len(evs["history"]) != 1 {
		t.Fatalf("events SSE: %d snapshots, %d history", len(evs["snapshot"]), len(evs["history"]))
	}

	// Live stream: backfill shows the produced records with ack info.
	msgs := readSSE(t, base+"/api/stream?topic=payments&backfill=10", 2*time.Second)
	found := 0
	for _, m := range msgs["message"] {
		if strings.Contains(m, `\"amount\":500`) && strings.Contains(m, `"key":"user-12"`) {
			found++
		}
	}
	if found != 3 {
		t.Fatalf("stream returned %d of 3 produced records: %v", found, msgs)
	}

	// A demo consumer group with one failing message -> DLQ.
	var cv ConsumerView
	if code := call(t, "POST", base+"/api/demo/consumers", AddConsumerRequest{Group: "billing", Topic: "payments"}, &cv); code/100 != 2 {
		t.Fatalf("add consumer: %d", code)
	}
	call(t, "POST", base+"/api/produce", ProduceRequest{Topic: "payments", Key: "poison", Value: `{"simulateFailure":true,"amount":1}`, Acks: "all"}, nil)

	var dlq struct {
		Entries []DLQEntry `json:"entries"`
	}
	testutil.Eventually(t, 15*time.Second, func() error {
		call(t, "GET", base+"/api/dlq", nil, &dlq)
		if len(dlq.Entries) != 1 {
			return fmt.Errorf("dlq entries %d", len(dlq.Entries))
		}
		return nil
	})
	e := dlq.Entries[0]
	if e.OriginalTopic != "payments" || e.Key != "poison" || !e.Eligible || e.ConsumerGroup != "billing" || !strings.Contains(e.Error, "simulateFailure") {
		t.Fatalf("dlq entry %+v", e)
	}
	// The group committed everything, including past the poison message.
	testutil.Eventually(t, 10*time.Second, func() error {
		for _, g := range getSnapshot(t, base).Groups {
			if g.ID == "billing" && g.State == "Stable" && g.TotalLag == 0 && len(g.Members) == 1 {
				return nil
			}
		}
		return fmt.Errorf("group billing not caught up")
	})

	fixed := `{"amount":1}`
	var rr ProduceResult
	if code := call(t, "POST", base+"/api/dlq/retry", RetryRequest{ID: e.ID, Payload: &fixed}, &rr); code != 200 || rr.Offset < 0 {
		t.Fatalf("retry: %d %+v", code, rr)
	}
	if code := call(t, "POST", base+"/api/dlq/retry", RetryRequest{ID: e.ID}, nil); code != 409 {
		t.Fatalf("second retry of the same entry: %d, want 409", code)
	}

	// Broker failure (attached mode: we kill it in-process). The event
	// stream must show the leader election and ISR shrink.
	tv, _ := findTopic(getSnapshot(t, base), "payments")
	victim := tv.Partitions[0].Leader
	cl.Kill(victim)
	testutil.Eventually(t, 20*time.Second, func() error {
		s := getSnapshot(t, base)
		var elected, shrunk, down bool
		for _, ev := range s.Recent {
			elected = elected || (ev.Kind == "leader_elected" && ev.Topic == "payments")
			shrunk = shrunk || (ev.Kind == "isr_shrink" && ev.Topic == "payments")
			down = down || (ev.Kind == "broker_unavailable" && ev.Broker == victim)
		}
		if !elected || !shrunk || !down {
			return fmt.Errorf("elected=%v shrunk=%v down=%v", elected, shrunk, down)
		}
		if s.Totals.OfflineBrokers != 1 {
			return fmt.Errorf("offline brokers %d", s.Totals.OfflineBrokers)
		}
		return nil
	})
	// Broker control is not offered in attached mode.
	if code := call(t, "POST", fmt.Sprintf("%s/api/cluster/brokers/%d/start", base, victim), nil, nil); code/100 == 2 {
		t.Fatalf("broker control accepted in attached mode: %d", code)
	}
}

// A crashed demo consumer must report "crashed" (regression: the dying run
// loop used to overwrite it with "running") and its partitions must move.
func TestDemoConsumerCrashStatusAndRebalance(t *testing.T) {
	_, _, base := startGateway(t)
	call(t, "POST", base+"/api/topics", CreateTopicRequest{Name: "jobs", Partitions: 4, ReplicationFactor: 3}, nil)
	var a, b ConsumerView
	call(t, "POST", base+"/api/demo/consumers", AddConsumerRequest{Group: "workers", Topic: "jobs"}, &a)
	call(t, "POST", base+"/api/demo/consumers", AddConsumerRequest{Group: "workers", Topic: "jobs"}, &b)
	testutil.Eventually(t, 15*time.Second, func() error {
		for _, g := range getSnapshot(t, base).Groups {
			if g.ID == "workers" && g.State == "Stable" && len(g.Members) == 2 {
				return nil
			}
		}
		return fmt.Errorf("group not stable with 2 members")
	})
	if code := call(t, "POST", base+"/api/demo/consumers/"+a.ID+"/crash", nil, nil); code/100 != 2 {
		t.Fatalf("crash: %d", code)
	}
	testutil.Eventually(t, 20*time.Second, func() error {
		s := getSnapshot(t, base)
		for _, c := range s.Demo.Consumers {
			if c.ID == a.ID && c.Status != "crashed" {
				return fmt.Errorf("crashed consumer reports %q", c.Status)
			}
		}
		for _, g := range s.Groups {
			if g.ID == "workers" && g.State == "Stable" && len(g.Members) == 1 && len(g.Members[0].Partitions) == 4 {
				return nil
			}
		}
		return fmt.Errorf("survivor does not own all 4 partitions yet")
	})
}

// Retry state is stored in the __dlq_retries topic, so a dashboard that
// restarts (here: a second, fresh gateway on the same cluster) still knows
// an entry was retried and refuses to re-publish it again.
func TestDLQRetryStateSurvivesDashboardRestart(t *testing.T) {
	cl, _, base := startGateway(t)
	call(t, "POST", base+"/api/topics", CreateTopicRequest{Name: "mail", Partitions: 1, ReplicationFactor: 3}, nil)
	call(t, "POST", base+"/api/demo/consumers", AddConsumerRequest{Group: "mailer", Topic: "mail"}, nil)
	call(t, "POST", base+"/api/produce", ProduceRequest{Topic: "mail", Key: "x", Value: `{"simulateFailure":true}`, Acks: "all"}, nil)
	var dlq struct {
		Entries []DLQEntry `json:"entries"`
	}
	testutil.Eventually(t, 15*time.Second, func() error {
		call(t, "GET", base+"/api/dlq", nil, &dlq)
		if len(dlq.Entries) != 1 {
			return fmt.Errorf("dlq entries %d", len(dlq.Entries))
		}
		return nil
	})
	id := dlq.Entries[0].ID
	ok := `{}`
	if code := call(t, "POST", base+"/api/dlq/retry", RetryRequest{ID: id, Payload: &ok}, nil); code != 200 {
		t.Fatalf("retry: %d", code)
	}

	g2, err := New(Config{Bootstrap: cl.Bootstrap(), Interval: 300 * time.Millisecond, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	srv2 := httptest.NewServer(g2.Handler())
	defer func() { srv2.Close(); g2.Close() }()
	testutil.Eventually(t, 15*time.Second, func() error {
		if s := g2.snapshot(); s == nil || !s.Reachable {
			return fmt.Errorf("second gateway not ready")
		}
		return nil
	})
	call(t, "GET", srv2.URL+"/api/dlq", nil, &dlq)
	if len(dlq.Entries) != 1 || !dlq.Entries[0].Retried || dlq.Entries[0].Eligible {
		t.Fatalf("fresh dashboard lost retry state: %+v", dlq.Entries)
	}
	if code := call(t, "POST", srv2.URL+"/api/dlq/retry", RetryRequest{ID: id}, nil); code != 409 {
		t.Fatalf("fresh dashboard allowed a second retry: %d", code)
	}
	// The retry log is internal: not listed among user topics or DLQs.
	for _, tv := range g2.snapshot().Topics {
		if tv.Name == RetryLogTopic && (!tv.Internal || tv.DLQ) {
			t.Fatalf("%s classified as %+v", RetryLogTopic, tv)
		}
	}
}

// Regression: while a group's coordinator fails over, the group used to
// drop out of the snapshot, so "messages consumed" fell and then jumped
// back, drawing a fake throughput spike. The consumed total must never go
// down. (A burst in consumed/s right after the failover is real: commits
// that could not be made during the outage are made at once.)
func TestConsumedTotalMonotonicDuringCoordinatorFailover(t *testing.T) {
	cl, g, base := startGateway(t)
	call(t, "POST", base+"/api/topics", CreateTopicRequest{Name: "ticks", Partitions: 2, ReplicationFactor: 3}, nil)
	call(t, "POST", base+"/api/demo/consumers", AddConsumerRequest{Group: "tickers", Topic: "ticks"}, nil)
	if code := call(t, "POST", base+"/api/demo/traffic", map[string]any{"running": true, "topic": "ticks", "rate": 20, "failureRate": 0}, nil); code/100 != 2 {
		t.Fatalf("traffic: %d", code)
	}
	testutil.Eventually(t, 20*time.Second, func() error {
		s := g.snapshot()
		for _, gv := range s.Groups {
			if gv.ID == "tickers" && gv.State == "Stable" && s.Totals.MessagesConsumed > 60 {
				return nil
			}
		}
		return fmt.Errorf("not consuming yet")
	})
	c := cl.Client()
	coord, err := c.FindCoordinator(context.Background(), "tickers")
	if err != nil {
		t.Fatal(err)
	}
	cl.Kill(coord.NodeID)
	var last int64 = -1
	staleSeen, maxRate := false, 0.0
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		s := g.snapshot()
		tot := s.Totals
		if tot.MessagesConsumed < last {
			t.Fatalf("messages consumed went down: %d -> %d", last, tot.MessagesConsumed)
		}
		if tot.MessagesConsumed > tot.MessagesProduced {
			t.Fatalf("consumed %d > produced %d", tot.MessagesConsumed, tot.MessagesProduced)
		}
		last = tot.MessagesConsumed
		maxRate = max(maxRate, tot.ConsumedPerSec)
		for _, gv := range s.Groups {
			staleSeen = staleSeen || (gv.ID == "tickers" && gv.Stale)
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Logf("killed coordinator %d: consumed total stayed monotonic (final %d); stale view used: %v; peak consumed/s %.0f (commit backlog flushed after failover)",
		coord.NodeID, last, staleSeen, maxRate)
	if !staleSeen {
		t.Log("note: coordinator failover was fast enough that no stale view was needed")
	}
}
