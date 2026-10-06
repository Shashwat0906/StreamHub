package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// Handler returns the dashboard's HTTP handler (API + UI).
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/snapshot", g.handleSnapshot)
	mux.HandleFunc("GET /api/events", g.handleEvents)
	mux.HandleFunc("POST /api/topics", g.handleCreateTopic)
	mux.HandleFunc("DELETE /api/topics/{name}", g.handleDeleteTopic)
	mux.HandleFunc("POST /api/produce", g.handleProduce)
	mux.HandleFunc("GET /api/stream", g.handleStream)
	mux.HandleFunc("GET /api/dlq", g.handleDLQ)
	mux.HandleFunc("POST /api/dlq/retry", g.handleDLQRetry)
	mux.HandleFunc("POST /api/demo/traffic", g.handleTraffic)
	mux.HandleFunc("POST /api/demo/consumers", g.handleAddConsumer)
	mux.HandleFunc("POST /api/demo/consumers/{id}/{action}", g.handleConsumerAction)
	mux.HandleFunc("DELETE /api/demo/consumers/{id}", g.handleRemoveConsumer)
	mux.HandleFunc("POST /api/cluster/brokers/{id}/{action}", g.handleBrokerAction)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "no such API endpoint")
	})
	if g.cfg.UI != nil {
		mux.Handle("/", spaHandler(g.cfg.UI))
	}
	return mux
}

// spaHandler serves the compiled UI; unknown paths get index.html.
func spaHandler(ui fs.FS) http.Handler {
	files := http.FileServer(http.FS(ui))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p != "" {
			if _, err := fs.Stat(ui, p); err != nil {
				r = r.Clone(r.Context())
				r.URL.Path = "/"
			}
		}
		if r.URL.Path == "/" {
			w.Header().Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func (g *Gateway) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s := g.snapshot()
	if s == nil {
		writeErr(w, http.StatusServiceUnavailable, "first snapshot not collected yet")
		return
	}
	cp := *s
	cp.Recent = g.recentEvents()
	writeJSON(w, http.StatusOK, cp)
}

// ---------------------------------------------------------------- SSE

func sseHeaders(w http.ResponseWriter) (http.Flusher, bool) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	return f, true
}

func writeSSE(w http.ResponseWriter, event string, data []byte) error {
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}

// handleEvents streams "snapshot" (once per second) and "event" (timeline)
// messages. On connect it sends the latest snapshot and recent events, so
// the page renders immediately without a separate request.
func (g *Gateway) handleEvents(w http.ResponseWriter, r *http.Request) {
	f, ok := sseHeaders(w)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	ch := g.hub.subscribe()
	defer g.hub.unsubscribe(ch)
	if s := g.snapshot(); s != nil {
		b, _ := json.Marshal(s)
		writeSSE(w, "snapshot", b)
	}
	b, _ := json.Marshal(g.recentEvents())
	writeSSE(w, "history", b)
	f.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-g.ctx.Done():
			return
		case m := <-ch:
			if writeSSE(w, m.event, m.data) != nil {
				return
			}
			f.Flush()
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			f.Flush()
		}
	}
}

// handleStream tails topics and streams records as "message" events.
//
//	GET /api/stream?topic=orders&partition=-1&backfill=50
//
// topic empty = every user topic (internal and DLQ topics excluded unless
// named explicitly). backfill = how many recent records per partition to
// send first.
func (g *Gateway) handleStream(w http.ResponseWriter, r *http.Request) {
	f, ok := sseHeaders(w)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	q := r.URL.Query()
	topic := q.Get("topic")
	partition := int32(-1)
	if v := q.Get("partition"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "bad partition")
			return
		}
		partition = int32(n)
	}
	backfill := int64(30)
	if v := q.Get("backfill"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 500 {
			backfill = int64(n)
		}
	}
	ctx := r.Context()
	if err := g.client.RefreshMetadata(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	snap := g.snapshot()
	if snap == nil {
		writeErr(w, http.StatusServiceUnavailable, "no snapshot yet")
		return
	}
	var targets []fetchTarget
	for _, t := range snap.Topics {
		if topic == "" && (t.Internal || t.DLQ) {
			continue
		}
		if topic != "" && t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			if partition >= 0 && p.ID != partition {
				continue
			}
			targets = append(targets, fetchTarget{t.Name, p.ID})
		}
	}
	if topic != "" && len(targets) == 0 {
		writeErr(w, http.StatusNotFound, "no such topic/partition")
		return
	}
	msgs := make(chan StreamMessage, 256)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t fetchTarget) {
			defer wg.Done()
			g.tail(ctx, t, backfill, msgs)
		}(t)
	}
	go func() { wg.Wait(); close(msgs) }()

	hello, _ := json.Marshal(map[string]any{"partitions": len(targets), "topic": topic, "partition": partition})
	writeSSE(w, "hello", hello)
	f.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-msgs:
			if !ok {
				return
			}
			b, _ := json.Marshal(m)
			if writeSSE(w, "message", b) != nil {
				return
			}
			// Coalesce bursts into one flush.
			for drained := 0; drained < 100; drained++ {
				select {
				case m2, ok := <-msgs:
					if !ok {
						f.Flush()
						return
					}
					b, _ := json.Marshal(m2)
					if writeSSE(w, "message", b) != nil {
						return
					}
					continue
				default:
				}
				break
			}
			f.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			f.Flush()
		}
	}
}

type fetchTarget struct {
	topic     string
	partition int32
}

const maxStreamValue = 4096

// tail follows one partition from (HW - backfill) onwards.
func (g *Gateway) tail(ctx context.Context, t fetchTarget, backfill int64, out chan<- StreamMessage) {
	var pc *client.PartitionConsumer
	for pc == nil {
		hw, err := g.client.ListOffsets(ctx, t.topic, t.partition, protocol.OffsetLatest)
		if err == nil {
			start, _ := g.client.ListOffsets(ctx, t.topic, t.partition, protocol.OffsetEarliest)
			pc, err = g.client.ConsumePartition(ctx, t.topic, t.partition, max(start, hw-backfill),
				client.FetchConfig{MaxWait: 500 * time.Millisecond, Reset: client.ResetLatest})
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second): // leader unavailable: try again
			}
		}
	}
	for ctx.Err() == nil {
		recs, err := pc.Poll(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		ackDeadline := time.Now().Add(300 * time.Millisecond) // one shared wait budget per batch
		for _, r := range recs {
			id := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
			ack := "committed to ISR (below high-watermark)"
			if d, ok := g.lookupAck(ctx, id, r.Timestamp, ackDeadline); ok {
				ack = d
			}
			v := r.Value
			m := StreamMessage{ID: id, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
				Timestamp: r.Timestamp.UnixMilli(), Key: string(r.Key), Size: len(r.Key) + len(r.Value), Ack: ack}
			if len(v) > maxStreamValue {
				v, m.Truncated = v[:maxStreamValue], true
			}
			m.Value = string(v)
			select {
			case out <- m:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ---------------------------------------------------------------- topics & produce

// CreateTopicRequest is the body of POST /api/topics.
type CreateTopicRequest struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs"`
}

func (g *Gateway) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err := g.client.CreateTopic(ctx, client.TopicSpec{Name: req.Name, Partitions: req.Partitions,
		ReplicationFactor: req.ReplicationFactor, Configs: req.Configs})
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"name": req.Name})
}

func (g *Gateway) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := g.client.DeleteTopic(ctx, r.PathValue("name")); err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// statusFor maps protocol errors to HTTP status codes.
func statusFor(err error) int {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		switch pe.Code {
		case protocol.ErrTopicAlreadyExists:
			return http.StatusConflict
		case protocol.ErrInvalidTopic, protocol.ErrInvalidRequest, protocol.ErrInvalidPartitions,
			protocol.ErrInvalidReplicationFactor, protocol.ErrMessageTooLarge:
			return http.StatusBadRequest
		case protocol.ErrUnknownTopicOrPartition:
			return http.StatusNotFound
		}
	}
	return http.StatusBadGateway
}

var producerMu sync.Mutex

// producerFor returns a cached producer for an acks mode.
func (g *Gateway) producerFor(acks string) (*client.Producer, error) {
	producerMu.Lock()
	defer producerMu.Unlock()
	if g.producers == nil {
		g.producers = map[string]*client.Producer{}
	}
	if p, ok := g.producers[acks]; ok {
		return p, nil
	}
	cfg := client.ProducerConfig{AcksSet: true, Linger: time.Millisecond, DeliveryTimeout: 20 * time.Second}
	switch acks {
	case "0":
		cfg.Acks = protocol.AcksNone
	case "1":
		cfg.Acks = protocol.AcksLeader
	case "all":
		cfg.Acks, cfg.Idempotent = protocol.AcksAll, true
	default:
		return nil, fmt.Errorf("acks must be 0, 1 or all")
	}
	p, err := g.client.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	g.producers[acks] = p
	return p, nil
}

// produce sends one record and reports how it was acknowledged.
func (g *Gateway) produce(ctx context.Context, req ProduceRequest) ProduceResult {
	if req.Acks == "" {
		req.Acks = "all"
	}
	res := ProduceResult{Topic: req.Topic, Acks: req.Acks, Partition: -1, Offset: -1, Timestamp: time.Now().UnixMilli()}
	if req.Topic == "" {
		res.Status, res.Error = "failed", "topic is required"
		return res
	}
	p, err := g.producerFor(req.Acks)
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	m := client.Message{Topic: req.Topic, Value: []byte(req.Value)}
	if req.Key != "" {
		m.Key = []byte(req.Key)
	}
	if req.Partition != nil && *req.Partition >= 0 {
		m.Partition, m.ManualPartition = *req.Partition, true
	}
	start := time.Now()
	d := p.SendSync(ctx, m)
	res.LatencyMs = float64(time.Since(start).Microseconds()) / 1000
	res.Partition = d.Partition
	if d.Err != nil {
		res.Status, res.Error = "failed", d.Err.Error()
		return res
	}
	res.Offset = d.Offset
	if req.Acks == "0" {
		res.Status = "sent-no-ack"
		res.MessageID = fmt.Sprintf("%s/%d/?", req.Topic, d.Partition)
		return res
	}
	res.Status = "acknowledged"
	res.MessageID = fmt.Sprintf("%s/%d/%d", req.Topic, d.Partition, d.Offset)
	desc := "acks=" + req.Acks
	if req.Acks == "all" {
		desc += " (idempotent)"
	}
	g.acks.put(res.MessageID, fmt.Sprintf("%s · %.1f ms · dashboard", desc, res.LatencyMs))
	return res
}

func (g *Gateway) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	res := g.produce(ctx, req)
	status := http.StatusOK
	if res.Status == "failed" {
		status = http.StatusBadGateway
		if strings.Contains(res.Error, "required") || strings.Contains(res.Error, "acks must") {
			status = http.StatusBadRequest
		}
	}
	writeJSON(w, status, res)
}

// ---------------------------------------------------------------- DLQ

func (g *Gateway) handleDLQ(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	entries, err := g.demo.listDLQ(ctx, 1000)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	if entries == nil {
		entries = []DLQEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "maxRetries": MaxDLQRetries})
}

func (g *Gateway) handleDLQRetry(w http.ResponseWriter, r *http.Request) {
	var req RetryRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	res, err := g.demo.retryDLQ(ctx, req)
	if err != nil {
		code := http.StatusBadGateway
		if strings.HasPrefix(err.Error(), "not eligible") || strings.HasPrefix(err.Error(), "bad message id") {
			code = http.StatusConflict
		}
		writeErr(w, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------- demo controls

// TrafficRequest is the body of POST /api/demo/traffic.
type TrafficRequest struct {
	Running     bool    `json:"running"`
	Topic       string  `json:"topic"`
	Rate        int     `json:"rate"`
	FailureRate float64 `json:"failureRate"`
}

func (g *Gateway) handleTraffic(w http.ResponseWriter, r *http.Request) {
	var req TrafficRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Rate < 0 || req.FailureRate < 0 || req.FailureRate > 1 {
		writeErr(w, http.StatusBadRequest, "rate must be >= 0 and failureRate in [0,1]")
		return
	}
	if req.Running && req.Topic != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		_, err := g.client.Topic(ctx, req.Topic)
		cancel()
		if err != nil {
			writeErr(w, http.StatusNotFound, "topic "+req.Topic+" does not exist")
			return
		}
	}
	g.demo.setTraffic(TrafficView{Running: req.Running, Topic: req.Topic, Rate: req.Rate, FailureRate: req.FailureRate})
	state := "stopped"
	if req.Running {
		state = fmt.Sprintf("started: %d msg/s to %s, %.0f%% marked to fail processing", g.demo.view().Traffic.Rate, g.demo.view().Traffic.Topic, 100*g.demo.view().Traffic.FailureRate)
	}
	g.emit(Event{Kind: "traffic", Severity: "info", Message: "Traffic generator " + state})
	writeJSON(w, http.StatusOK, g.demo.view().Traffic)
}

func (g *Gateway) handleAddConsumer(w http.ResponseWriter, r *http.Request) {
	var req AddConsumerRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Strategy != "" && req.Strategy != "range" && req.Strategy != "roundrobin" {
		writeErr(w, http.StatusBadRequest, "strategy must be range or roundrobin")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	_, err := g.client.Topic(ctx, req.Topic)
	cancel()
	if err != nil {
		writeErr(w, http.StatusNotFound, "topic "+req.Topic+" does not exist")
		return
	}
	c, err := g.demo.addConsumer(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, c.view())
}

func (g *Gateway) handleConsumerAction(w http.ResponseWriter, r *http.Request) {
	c, ok := g.demo.consumer(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "no such consumer")
		return
	}
	switch r.PathValue("action") {
	case "crash":
		c.crash()
	case "stop":
		c.stop(true)
		g.emit(Event{Kind: "consumer_stopped", Severity: "info", Group: c.group,
			Message: fmt.Sprintf("Demo %s left group %s gracefully (committed, LeaveGroup): rebalance starts immediately", c.id, c.group)})
	case "restart":
		v := c.view()
		if v.Status != "crashed" && v.Status != "stopped" {
			writeErr(w, http.StatusConflict, "consumer is still running")
			return
		}
		if err := c.start(); err != nil {
			writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		g.emit(Event{Kind: "consumer_started", Phase: PhaseRecovery, Severity: "success", Group: c.group,
			Message: fmt.Sprintf("Demo %s restarted and is rejoining group %s", c.id, c.group)})
	default:
		writeErr(w, http.StatusBadRequest, "action must be crash, stop or restart")
		return
	}
	writeJSON(w, http.StatusOK, c.view())
}

func (g *Gateway) handleRemoveConsumer(w http.ResponseWriter, r *http.Request) {
	if err := g.demo.removeConsumer(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) handleBrokerAction(w http.ResponseWriter, r *http.Request) {
	if g.mgr == nil {
		writeErr(w, http.StatusNotImplemented, g.capabilities().Note)
		return
	}
	n, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad broker id")
		return
	}
	id := int32(n)
	action := r.PathValue("action")
	var msg, kind, sev, phase string
	switch action {
	case "kill":
		err = g.mgr.kill(id)
		kind, sev, phase = "broker_killed", "error", PhaseUnavailable
		msg = fmt.Sprintf("Broker %d killed with SIGKILL (simulated crash: no controlled shutdown)", id)
	case "stop":
		err = g.mgr.stop(id)
		kind, sev = "broker_stopped", "warn"
		msg = fmt.Sprintf("Broker %d stopped with SIGTERM (controlled shutdown: leaders moved first)", id)
	case "start":
		err = g.mgr.start(id)
		kind, sev, phase = "broker_started", "info", PhaseRecovery
		msg = fmt.Sprintf("Broker %d process started; it will catch up and rejoin the ISR", id)
	default:
		writeErr(w, http.StatusBadRequest, "action must be kill, stop or start")
		return
	}
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	g.emit(Event{Kind: kind, Phase: phase, Severity: sev, Broker: id, Message: msg})
	writeJSON(w, http.StatusOK, map[string]any{"broker": id, "action": action, "process": g.mgr.view(id)})
}

// lookupAck returns how a dashboard-produced record was acknowledged. A
// consumer can see a record (HW advanced) a moment before the producer's
// ack callback runs, so for recent records we wait for it, but only until
// deadline, which the caller shares across a whole fetched batch so that
// records from other producers (never in the log) cost at most one wait.
func (g *Gateway) lookupAck(ctx context.Context, id string, ts time.Time, deadline time.Time) (string, bool) {
	for {
		if d, ok := g.acks.get(id); ok {
			return d, true
		}
		if time.Now().After(deadline) || time.Since(ts) > 3*time.Second {
			return "", false
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(25 * time.Millisecond):
		}
	}
}
