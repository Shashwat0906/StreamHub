package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// MaxDLQRetries is how many times a dead-lettered message may be re-published
// from the dashboard before it is no longer eligible.
const MaxDLQRetries = 3

// processingAttempts is how many times a demo consumer tries a record
// in-process before dead-lettering it.
const processingAttempts = 3

// dlqEnvelope is the value of a record in a "<topic>.dlq" topic. StreamHub
// records have no headers, so the failure metadata travels in the value.
type dlqEnvelope struct {
	OriginalTopic string `json:"originalTopic"`
	Partition     int32  `json:"partition"`
	Offset        int64  `json:"offset"`
	Key           string `json:"key"`
	Payload       string `json:"payload"`
	Error         string `json:"error"`
	Attempts      int    `json:"attempts"`
	RetryCount    int    `json:"retryCount"` // dashboard re-publishes before this failure
	ConsumerGroup string `json:"consumerGroup"`
	FailedAt      int64  `json:"failedAt"`
}

// demo owns the traffic generator and the demo consumers.
type demo struct {
	g *Gateway

	mu        sync.Mutex
	traffic   TrafficView
	stopTraf  context.CancelFunc
	trafDone  chan struct{}
	consumers map[string]*demoConsumer
	nextID    int
	dlqTopics map[string]bool // DLQ topics known to exist

	failed    atomic.Int64
	sent      atomic.Int64
	trafErrs  atomic.Int64
	dlqRecent []string        // "topic/partition/offset" of dead-lettered originals
	retried   map[string]bool // DLQ entry IDs already re-published
	priorRetr map[string]int  // message fingerprint -> times re-published
	seeded    bool

	retryMu        sync.Mutex
	retryLogMu     sync.Mutex
	retryLogLoaded bool
}

func newDemo(g *Gateway) *demo {
	return &demo{g: g, consumers: map[string]*demoConsumer{}, dlqTopics: map[string]bool{},
		retried: map[string]bool{}, priorRetr: map[string]int{},
		traffic: TrafficView{Topic: "orders", Rate: 20, FailureRate: 0.05}}
}

func (d *demo) failures() int64 { return d.failed.Load() }

func (d *demo) dlqIndex() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string{}, d.dlqRecent...)
}

func (d *demo) noteDLQ(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dlqRecent = append(d.dlqRecent, id)
	if len(d.dlqRecent) > 500 {
		d.dlqRecent = d.dlqRecent[len(d.dlqRecent)-500:]
	}
}

func (d *demo) view() DemoView {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := DemoView{Traffic: d.traffic, Consumers: []ConsumerView{}}
	v.Traffic.Sent = d.sent.Load()
	v.Traffic.Errors = d.trafErrs.Load()
	ids := make([]string, 0, len(d.consumers))
	for id := range d.consumers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return consumerNum(ids[i]) < consumerNum(ids[j]) })
	for _, id := range ids {
		v.Consumers = append(v.Consumers, d.consumers[id].view())
	}
	return v
}

func consumerNum(id string) int {
	n, _ := strconv.Atoi(id[len("consumer-"):])
	return n
}

func (d *demo) closeAll() {
	d.setTraffic(TrafficView{Running: false})
	d.mu.Lock()
	cs := make([]*demoConsumer, 0, len(d.consumers))
	for _, c := range d.consumers {
		cs = append(cs, c)
	}
	d.mu.Unlock()
	for _, c := range cs {
		c.stop(true)
	}
}

// ---------------------------------------------------------------- failure rule

// shouldFail is the demo processing rule: a JSON object with
// "simulateFailure": true cannot be processed. Everything else succeeds.
func shouldFail(value []byte) (bool, string) {
	var obj map[string]any
	if json.Unmarshal(value, &obj) != nil {
		return false, ""
	}
	if v, ok := obj["simulateFailure"].(bool); ok && v {
		return true, "processing rejected: payload has simulateFailure=true"
	}
	return false, ""
}

func fingerprint(topic, key, payload string) string {
	h := sha256.Sum256([]byte(topic + "\x00" + key + "\x00" + payload))
	return hex.EncodeToString(h[:8])
}

// ---------------------------------------------------------------- traffic

// setTraffic starts, stops or reconfigures the traffic generator.
func (d *demo) setTraffic(want TrafficView) error {
	d.mu.Lock()
	if d.stopTraf != nil {
		d.stopTraf()
		done := d.trafDone
		d.stopTraf, d.trafDone = nil, nil
		d.mu.Unlock()
		<-done
		d.mu.Lock()
	}
	if want.Topic != "" {
		d.traffic.Topic = want.Topic
	}
	if want.Rate > 0 {
		d.traffic.Rate = min(want.Rate, 5000)
	}
	if want.FailureRate >= 0 && want.FailureRate <= 1 {
		d.traffic.FailureRate = want.FailureRate
	}
	d.traffic.Running = want.Running
	d.traffic.LastError = ""
	if !want.Running {
		d.mu.Unlock()
		return nil
	}
	cfg := d.traffic
	ctx, cancel := context.WithCancel(d.g.ctx)
	d.stopTraf, d.trafDone = cancel, make(chan struct{})
	done := d.trafDone
	d.mu.Unlock()
	go d.runTraffic(ctx, cfg, done)
	return nil
}

func (d *demo) runTraffic(ctx context.Context, cfg TrafficView, done chan struct{}) {
	defer close(done)
	p, err := d.g.client.NewProducer(client.ProducerConfig{Idempotent: true, Linger: 10 * time.Millisecond, DeliveryTimeout: 15 * time.Second})
	if err != nil {
		d.trafficError(err)
		return
	}
	defer p.Close()
	tick := 100 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	carry := 0.0
	order := time.Now().Unix() % 100000 * 1000
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		carry += float64(cfg.Rate) * tick.Seconds()
		n := int(carry)
		carry -= float64(n)
		for i := 0; i < n; i++ {
			order++
			user := rand.Intn(50) + 1
			payload := map[string]any{
				"orderId": order, "userId": user, "amount": rand.Intn(5000) + 100,
				"currency": "INR", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
			}
			if rand.Float64() < cfg.FailureRate {
				payload["simulateFailure"] = true
			}
			b, _ := json.Marshal(payload)
			m := client.Message{Topic: cfg.Topic, Key: []byte("user-" + strconv.Itoa(user)), Value: b}
			if err := p.Send(ctx, m, func(dl client.Delivery) {
				if dl.Err != nil {
					d.trafficError(dl.Err)
					return
				}
				d.sent.Add(1)
				d.g.acks.put(fmt.Sprintf("%s/%d/%d", dl.Topic, dl.Partition, dl.Offset), "acks=all (traffic generator)")
			}); err != nil && ctx.Err() == nil {
				d.trafficError(err)
			}
		}
	}
}

func (d *demo) trafficError(err error) {
	d.trafErrs.Add(1)
	d.mu.Lock()
	d.traffic.LastError = err.Error()
	d.mu.Unlock()
}

// ---------------------------------------------------------------- consumers

// demoConsumer is a real consumer-group member with its own client (so a
// "crash" really drops its connections and stops its heartbeats).
type demoConsumer struct {
	d                 *demo
	id, group, topic  string
	strategy          string
	delay             time.Duration
	mu                sync.Mutex
	status            string
	cl                *client.Client
	gc                *client.GroupConsumer
	processed, failed atomic.Int64
	lastErr           string
	cancel            context.CancelFunc
	done              chan struct{}
	dlqProducer       *client.Producer
}

// AddConsumerRequest is the body of POST /api/demo/consumers.
type AddConsumerRequest struct {
	Group    string `json:"group"`
	Topic    string `json:"topic"`
	DelayMs  int    `json:"delayMs"`
	Strategy string `json:"strategy"`
}

func (d *demo) addConsumer(req AddConsumerRequest) (*demoConsumer, error) {
	if req.Group == "" || req.Topic == "" {
		return nil, errors.New("group and topic are required")
	}
	if req.DelayMs < 0 || req.DelayMs > 10000 {
		return nil, errors.New("delayMs must be between 0 and 10000")
	}
	d.mu.Lock()
	d.nextID++
	c := &demoConsumer{d: d, id: fmt.Sprintf("consumer-%d", d.nextID), group: req.Group, topic: req.Topic,
		strategy: req.Strategy, delay: time.Duration(req.DelayMs) * time.Millisecond}
	d.consumers[c.id] = c
	d.mu.Unlock()
	if err := c.start(); err != nil {
		return nil, err
	}
	d.g.emit(Event{Kind: "consumer_started", Severity: "info", Group: req.Group,
		Message: fmt.Sprintf("Demo %s joined group %s on topic %s", c.id, req.Group, req.Topic)})
	return c, nil
}

func (d *demo) consumer(id string) (*demoConsumer, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.consumers[id]
	return c, ok
}

func (d *demo) removeConsumer(id string) error {
	c, ok := d.consumer(id)
	if !ok {
		return errors.New("no such consumer")
	}
	c.stop(true)
	d.mu.Lock()
	delete(d.consumers, id)
	d.mu.Unlock()
	return nil
}

func (c *demoConsumer) start() error {
	cl, err := client.New(client.Config{Bootstrap: c.d.g.bootstrap(), ClientID: c.id,
		RequestTimeout: 5 * time.Second, MetadataMaxAge: 2 * time.Second, Dialer: c.d.g.cfg.Dialer})
	if err != nil {
		return err
	}
	// Session timeout 6 s: long enough to survive a broker failover, short
	// enough that a crashed consumer is noticed quickly in a demo.
	gc, err := cl.NewGroupConsumer(client.GroupConfig{
		Group: c.group, Topics: []string{c.topic}, Strategy: c.strategy,
		SessionTimeout: 6 * time.Second, HeartbeatInterval: time.Second, RebalanceTimeout: 15 * time.Second,
		Fetch: client.FetchConfig{MaxWait: 300 * time.Millisecond},
	})
	if err != nil {
		cl.Close()
		return err
	}
	dlqp, err := cl.NewProducer(client.ProducerConfig{Idempotent: true, DeliveryTimeout: 20 * time.Second})
	if err != nil {
		cl.Close()
		return err
	}
	ctx, cancel := context.WithCancel(c.d.g.ctx)
	c.mu.Lock()
	c.cl, c.gc, c.dlqProducer, c.cancel, c.done, c.status, c.lastErr = cl, gc, dlqp, cancel, make(chan struct{}), "starting", ""
	done := c.done
	c.mu.Unlock()
	go c.run(ctx, gc, done)
	return nil
}

func (c *demoConsumer) clearError() {
	c.mu.Lock()
	c.lastErr = ""
	c.mu.Unlock()
}

func (c *demoConsumer) setStatus(s, errMsg string) {
	c.mu.Lock()
	// crashed/stopped are terminal: the dying run loop must not flip a
	// crashed consumer back to "running" while it unwinds.
	if c.status == "crashed" || c.status == "stopped" {
		c.mu.Unlock()
		return
	}
	c.status = s
	if errMsg != "" {
		c.lastErr = errMsg
	}
	c.mu.Unlock()
}

func (c *demoConsumer) view() ConsumerView {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := ConsumerView{ID: c.id, Group: c.group, Topic: c.topic, Status: c.status,
		DelayMs: int(c.delay / time.Millisecond), Processed: c.processed.Load(), Failed: c.failed.Load(),
		LastError: c.lastErr, Partitions: []string{}}
	if c.gc != nil && (c.status == "running" || c.status == "starting") {
		v.MemberID = c.gc.MemberID()
		v.Generation = c.gc.Generation()
		for _, tp := range c.gc.Assignment() {
			for _, p := range tp.Partitions {
				v.Partitions = append(v.Partitions, tpKey(tp.Topic, p))
			}
		}
	}
	return v
}

func (c *demoConsumer) run(ctx context.Context, gc *client.GroupConsumer, done chan struct{}) {
	defer close(done)
	for ctx.Err() == nil {
		recs, err := gc.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.setStatus("starting", err.Error())
			time.Sleep(200 * time.Millisecond)
			continue
		}
		c.setStatus("running", "")
		for i, r := range recs {
			if c.delay > 0 {
				select {
				case <-ctx.Done():
					// Unprocessed records must not be committed.
					c.rewind(gc, recs[i:])
					return
				case <-time.After(c.delay):
				}
			}
			if !c.process(ctx, r) {
				// The record could not be processed or dead-lettered. Do not
				// commit past it: rewind so it is delivered again
				// (at-least-once), commit what came before, back off.
				c.rewind(gc, recs[i:])
				break
			}
		}
		if len(recs) > 0 {
			if err := gc.Commit(ctx); err != nil && ctx.Err() == nil {
				c.setStatus("running", "commit: "+err.Error())
			} else if err == nil {
				c.clearError() // recovered: do not keep showing a transient failure
			}
		}
	}
}

// rewind seeks each partition back to its first unprocessed record.
func (c *demoConsumer) rewind(gc *client.GroupConsumer, rest []client.Record) {
	done := map[string]bool{}
	for _, r := range rest {
		k := tpKey(r.Topic, r.Partition)
		if !done[k] {
			gc.Seek(r.Topic, r.Partition, r.Offset)
			done[k] = true
		}
	}
}

// process handles one record and reports whether it is done with it
// (processed, or safely written to the DLQ). false means "retry later".
func (c *demoConsumer) process(ctx context.Context, r client.Record) bool {
	var reason string
	failed := false
	for attempt := 1; attempt <= processingAttempts; attempt++ {
		failed, reason = shouldFail(r.Value)
		if !failed {
			break
		}
		if attempt < processingAttempts {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !failed {
		c.processed.Add(1)
		return true
	}
	if err := c.deadLetter(ctx, r, reason); err != nil {
		if ctx.Err() == nil {
			c.setStatus("running", "dead-letter failed, will retry: "+err.Error())
			time.Sleep(500 * time.Millisecond)
		}
		return false
	}
	c.failed.Add(1)
	c.d.failed.Add(1)
	return true
}

// deadLetter writes the failed record to "<topic>.dlq" with acks=all.
func (c *demoConsumer) deadLetter(ctx context.Context, r client.Record, reason string) error {
	dlq := r.Topic + dlqSuffix
	if err := c.d.ensureDLQTopic(ctx, dlq); err != nil {
		return err
	}
	c.d.mu.Lock()
	prior := c.d.priorRetr[fingerprint(r.Topic, string(r.Key), string(r.Value))]
	c.d.mu.Unlock()
	env := dlqEnvelope{OriginalTopic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key),
		Payload: string(r.Value), Error: reason, Attempts: processingAttempts, RetryCount: prior,
		ConsumerGroup: c.group, FailedAt: time.Now().UnixMilli()}
	b, _ := json.Marshal(env)
	c.mu.Lock()
	p := c.dlqProducer
	c.mu.Unlock()
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	d := p.SendSync(dctx, client.Message{Topic: dlq, Key: r.Key, Value: b})
	if d.Err != nil {
		return d.Err
	}
	c.d.noteDLQ(fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset))
	return nil
}

// crash simulates a consumer process dying: connections are closed
// without committing or leaving the group, so the coordinator only
// notices when the session times out, and then rebalances.
func (c *demoConsumer) crash() {
	c.mu.Lock()
	if c.status == "crashed" || c.status == "stopped" {
		c.mu.Unlock()
		return
	}
	cl, cancel, done := c.cl, c.cancel, c.done
	c.status = "crashed"
	c.mu.Unlock()
	cl.Close()
	cancel()
	<-done
	c.d.g.emit(Event{Kind: "consumer_crashed", Phase: PhaseUnavailable, Severity: "error", Group: c.group,
		Message: fmt.Sprintf("Demo %s crashed (no commit, no LeaveGroup): the coordinator will notice after the 6 s session timeout", c.id)})
}

// stop shuts the consumer down; graceful = commit + LeaveGroup.
func (c *demoConsumer) stop(graceful bool) {
	c.mu.Lock()
	if c.status == "crashed" || c.status == "stopped" {
		c.mu.Unlock()
		return
	}
	cl, gc, cancel, done, p := c.cl, c.gc, c.cancel, c.done, c.dlqProducer
	c.status = "stopped"
	c.mu.Unlock()
	cancel()
	<-done
	if graceful {
		ctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		gc.Close(ctx)
		ccancel()
	}
	p.Close()
	cl.Close()
}

// ---------------------------------------------------------------- DLQ

// ensureDLQTopic creates a DLQ topic on first use.
func (d *demo) ensureDLQTopic(ctx context.Context, name string) error {
	return d.ensureTopic(ctx, name)
}

// ensureTopic creates a 1-partition helper topic (DLQ or retry log) with
// RF = min(3, healthy brokers) if it does not exist.
func (d *demo) ensureTopic(ctx context.Context, name string) error {
	d.mu.Lock()
	known := d.dlqTopics[name]
	d.mu.Unlock()
	if known {
		return nil
	}
	if _, err := d.g.client.Topic(ctx, name); err == nil {
		d.mu.Lock()
		d.dlqTopics[name] = true
		d.mu.Unlock()
		return nil
	}
	live := 0
	if s := d.g.snapshot(); s != nil {
		live = s.Totals.HealthyBrokers
	}
	rf := int16(min(3, max(1, live)))
	err := d.g.client.CreateTopic(ctx, client.TopicSpec{Name: name, Partitions: 1, ReplicationFactor: rf})
	var pe *protocol.Error
	if err != nil && !(errors.As(err, &pe) && pe.Code == protocol.ErrTopicAlreadyExists) {
		return err
	}
	d.mu.Lock()
	d.dlqTopics[name] = true
	d.mu.Unlock()
	return nil
}

// listDLQ reads every "*.dlq" topic (newest last maxPerTopic records).
func (d *demo) listDLQ(ctx context.Context, maxPerTopic int) ([]DLQEntry, error) {
	if err := d.g.client.RefreshMetadata(ctx); err != nil {
		return nil, err
	}
	if err := d.loadRetryLog(ctx); err != nil {
		return nil, fmt.Errorf("loading %s: %w", RetryLogTopic, err)
	}
	snap := d.g.snapshot()
	var out []DLQEntry
	if snap == nil {
		return out, nil
	}
	for _, t := range snap.Topics {
		if !t.DLQ {
			continue
		}
		for _, p := range t.Partitions {
			entries, err := d.readDLQPartition(ctx, t.Name, p.ID, maxPerTopic)
			if err != nil {
				return out, fmt.Errorf("%s-%d: %w", t.Name, p.ID, err)
			}
			out = append(out, entries...)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FailedAt > out[j].FailedAt })
	return out, nil
}

func (d *demo) readDLQPartition(ctx context.Context, topic string, part int32, max int) ([]DLQEntry, error) {
	end, err := d.g.client.ListOffsets(ctx, topic, part, protocol.OffsetLatest)
	if err != nil {
		return nil, err
	}
	start, err := d.g.client.ListOffsets(ctx, topic, part, protocol.OffsetEarliest)
	if err != nil {
		return nil, err
	}
	if end-start > int64(max) {
		start = end - int64(max)
	}
	if start >= end {
		return nil, nil
	}
	pc, err := d.g.client.ConsumePartition(ctx, topic, part, start, client.FetchConfig{MaxWait: 50 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	var out []DLQEntry
	for pc.Offset() < end {
		recs, err := pc.Poll(ctx)
		if err != nil {
			return out, err
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			if r.Offset >= end {
				break
			}
			out = append(out, d.toEntry(topic, part, r))
		}
	}
	return out, nil
}

func (d *demo) toEntry(topic string, part int32, r client.Record) DLQEntry {
	id := fmt.Sprintf("%s/%d/%d", topic, part, r.Offset)
	e := DLQEntry{ID: id, DLQTopic: topic, DLQPartition: part, DLQOffset: r.Offset, Key: string(r.Key), FailedAt: r.Timestamp.UnixMilli()}
	var env dlqEnvelope
	if err := json.Unmarshal(r.Value, &env); err != nil || env.OriginalTopic == "" {
		// Not written by the dashboard: show it, but it cannot be retried.
		e.Payload, e.Error, e.Partition, e.Offset = string(r.Value), "unknown (record is not a StreamHub DLQ envelope)", -1, -1
		e.IneligibleCause = "unknown format"
		return e
	}
	e.OriginalTopic, e.Partition, e.Offset, e.Key = env.OriginalTopic, env.Partition, env.Offset, env.Key
	e.Payload, e.Error, e.RetryCount, e.ConsumerGroup, e.FailedAt = env.Payload, env.Error, env.RetryCount, env.ConsumerGroup, env.FailedAt
	d.mu.Lock()
	e.Retried = d.retried[id]
	d.mu.Unlock()
	switch {
	case e.Retried:
		e.IneligibleCause = "already retried"
	case e.RetryCount >= MaxDLQRetries:
		e.IneligibleCause = fmt.Sprintf("retry limit (%d) reached", MaxDLQRetries)
	default:
		e.Eligible = true
	}
	return e
}

// RetryRequest is the body of POST /api/dlq/retry.
type RetryRequest struct {
	ID      string  `json:"id"`      // dlqTopic/partition/offset
	Payload *string `json:"payload"` // optional replacement payload
}

// retryDLQ re-publishes a dead-lettered message to its original topic.
func (d *demo) retryDLQ(ctx context.Context, req RetryRequest) (ProduceResult, error) {
	topic, part, off, err := parseID(req.ID)
	if err != nil {
		return ProduceResult{}, err
	}
	if err := d.loadRetryLog(ctx); err != nil {
		return ProduceResult{}, fmt.Errorf("loading %s: %w", RetryLogTopic, err)
	}
	d.retryMu.Lock() // one retry at a time: two clicks must not both pass the eligibility check
	defer d.retryMu.Unlock()
	e, err := d.readDLQPartitionAt(ctx, topic, part, off)
	if err != nil {
		return ProduceResult{}, err
	}
	if !e.Eligible {
		return ProduceResult{}, fmt.Errorf("not eligible for retry: %s", e.IneligibleCause)
	}
	payload := e.Payload
	if req.Payload != nil {
		payload = *req.Payload
	}
	res := d.g.produce(ctx, ProduceRequest{Topic: e.OriginalTopic, Key: e.Key, Value: payload, Acks: "all"})
	if res.Status != "acknowledged" {
		return res, errors.New(res.Error)
	}
	fp := fingerprint(e.OriginalTopic, e.Key, payload)
	d.mu.Lock()
	d.retried[req.ID] = true
	d.priorRetr[fp] = e.RetryCount + 1
	d.mu.Unlock()
	if err := d.recordRetry(ctx, retryMark{ID: req.ID, Fingerprint: fp, RetryCount: e.RetryCount + 1, NewMessage: res.MessageID, At: time.Now().UnixMilli()}); err != nil {
		// Re-published, but the mark is only in memory until a later write.
		d.g.log.Warn("DLQ retry mark not persisted", "id", req.ID, "err", err)
	}
	d.g.emit(Event{Kind: "dlq_retry", Severity: "info", Topic: e.OriginalTopic,
		Message: fmt.Sprintf("DLQ message %s re-published to %s as %s (retry %d of %d)", req.ID, e.OriginalTopic, res.MessageID, e.RetryCount+1, MaxDLQRetries)})
	return res, nil
}

func (d *demo) readDLQPartitionAt(ctx context.Context, topic string, part int32, off int64) (DLQEntry, error) {
	pc, err := d.g.client.ConsumePartition(ctx, topic, part, off, client.FetchConfig{MaxWait: 100 * time.Millisecond, MaxBytes: 64 << 10})
	if err != nil {
		return DLQEntry{}, err
	}
	recs, err := pc.Poll(ctx)
	if err != nil {
		return DLQEntry{}, err
	}
	for _, r := range recs {
		if r.Offset == off {
			return d.toEntry(topic, part, r), nil
		}
	}
	return DLQEntry{}, fmt.Errorf("DLQ record %s/%d/%d not found", topic, part, off)
}

// seedDLQIndex loads dead-lettered originals from existing DLQ topics once
// so the live stream can mark them after a dashboard restart.
func (d *demo) seedDLQIndex(ctx context.Context) {
	d.mu.Lock()
	if d.seeded {
		d.mu.Unlock()
		return
	}
	d.seeded = true
	d.mu.Unlock()
	entries, err := d.listDLQ(ctx, 500)
	if err != nil {
		return
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].OriginalTopic != "" {
			d.noteDLQ(fmt.Sprintf("%s/%d/%d", entries[i].OriginalTopic, entries[i].Partition, entries[i].Offset))
		}
	}
}

// parseID splits "topic/partition/offset".
func parseID(id string) (string, int32, int64, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 3 {
		return "", 0, 0, fmt.Errorf("bad message id %q (want topic/partition/offset)", id)
	}
	p, err1 := strconv.ParseInt(parts[1], 10, 32)
	o, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil {
		return "", 0, 0, fmt.Errorf("bad message id %q", id)
	}
	return parts[0], int32(p), o, nil
}
