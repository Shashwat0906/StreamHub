package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/transport"
)

// Config configures the dashboard gateway.
type Config struct {
	// Bootstrap lists broker protocol addresses (attached mode). In managed
	// mode it is filled in from the launched brokers.
	Bootstrap []string
	// Managed, if set, makes the dashboard launch and own a local cluster
	// of broker processes, which enables broker failure simulation.
	Managed *ManagedConfig
	// Interval between snapshots (default 1s).
	Interval time.Duration
	// UI is the compiled web app (nil = API only).
	UI     fs.FS
	Dialer *transport.Dialer // tests
	Logger *slog.Logger
}

// Gateway is the dashboard backend.
type Gateway struct {
	cfg    Config
	log    *slog.Logger
	client *client.Client
	httpc  *http.Client
	coll   *collector
	demo   *demo
	mgr    *manager
	hub    *hub
	acks   *ackLog

	producers map[string]*client.Producer // per acks mode, guarded by producerMu

	mu       sync.RWMutex
	snap     *Snapshot
	events   []Event
	nextEvID int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a gateway. In managed mode it launches the brokers.
func New(cfg Config) (*Gateway, error) {
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{
		cfg:    cfg,
		log:    cfg.Logger.With("component", "dashboard"),
		httpc:  &http.Client{Timeout: 2 * time.Second},
		hub:    newHub(),
		acks:   newAckLog(5000),
		ctx:    ctx,
		cancel: cancel,
	}
	if cfg.Managed != nil {
		mgr, err := newManager(g, *cfg.Managed)
		if err != nil {
			cancel()
			return nil, err
		}
		g.mgr = mgr
		if err := mgr.startAll(); err != nil {
			mgr.stopAll()
			cancel()
			return nil, err
		}
		g.cfg.Bootstrap = mgr.bootstrap()
	}
	cl, err := client.New(client.Config{
		Bootstrap: g.cfg.Bootstrap, ClientID: "streamhub-dashboard",
		RequestTimeout: 5 * time.Second, MetadataMaxAge: 2 * time.Second, Dialer: cfg.Dialer,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	g.client = cl
	g.coll = newCollector(g)
	g.demo = newDemo(g)
	g.wg.Add(1)
	go g.loop()
	return g, nil
}

// Close stops demo workers, the collector and (managed mode) the brokers.
func (g *Gateway) Close() {
	g.cancel()
	g.wg.Wait()
	g.demo.closeAll()
	producerMu.Lock()
	for _, p := range g.producers {
		p.Close()
	}
	producerMu.Unlock()
	if g.mgr != nil {
		g.mgr.stopAll()
	}
	g.client.Close()
}

func (g *Gateway) bootstrap() []string { return g.cfg.Bootstrap }

func (g *Gateway) mode() string {
	if g.mgr != nil {
		return "managed"
	}
	return "attached"
}

func (g *Gateway) capabilities() Capabilities {
	c := Capabilities{ConsumerControl: true, Traffic: true, BrokerControl: g.mgr != nil}
	if g.mgr == nil {
		c.Note = "Broker failure simulation needs the dashboard to own the broker processes: start it with `streamhub dashboard --managed`. Consumer failures and traffic work in both modes."
	}
	return c
}

func (g *Gateway) managedHTTPAddrs() map[int32]string {
	if g.mgr == nil {
		return nil
	}
	return g.mgr.httpAddrs()
}

func (g *Gateway) managedAddr(id int32) string {
	if g.mgr == nil {
		return ""
	}
	return g.mgr.addr(id)
}

func (g *Gateway) processView(id int32) *ProcessView {
	if g.mgr == nil {
		return nil
	}
	return g.mgr.view(id)
}

// offlineBrokerViews is used when no broker answers at all.
func (g *Gateway) offlineBrokerViews(prev *Snapshot) []BrokerView {
	var out []BrokerView
	if prev != nil {
		for _, b := range prev.Brokers {
			b.Status, b.Reachable, b.StatusDetail = "offline", false, "unreachable"
			b.InMsgPerSec, b.OutMsgPerSec = 0, 0
			b.Process = g.processView(b.ID)
			out = append(out, b)
		}
	}
	if len(out) == 0 && g.mgr != nil {
		for id, addr := range g.mgr.httpAddrs() {
			out = append(out, BrokerView{ID: id, Addr: g.mgr.addr(id), HTTPAddr: addr, Status: "offline",
				StatusDetail: "unreachable", LeaderPartitions: []string{}, ReplicaPartitions: []string{}, Process: g.mgr.view(id)})
		}
	}
	if out == nil {
		out = []BrokerView{}
	}
	return out
}

// loop collects a snapshot every interval, diffs it against the previous
// one and broadcasts both the snapshot and any new events.
func (g *Gateway) loop() {
	defer g.wg.Done()
	t := time.NewTicker(g.cfg.Interval)
	defer t.Stop()
	for {
		g.tick()
		select {
		case <-g.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (g *Gateway) tick() {
	g.mu.RLock()
	prev := g.snap
	g.mu.RUnlock()
	ctx, cancel := context.WithTimeout(g.ctx, 4*time.Second)
	snap := g.coll.collect(ctx, prev)
	cancel()
	for _, e := range diffSnapshots(prev, snap) {
		g.emit(e)
	}
	g.mu.Lock()
	g.snap = snap
	g.mu.Unlock()
	if snap.Reachable && (prev == nil || !prev.Reachable) {
		go g.demo.seedDLQIndex(g.ctx)
	}
	if b, err := json.Marshal(snap); err == nil {
		g.hub.publish("snapshot", b)
	}
}

// emit records a timeline event and pushes it to subscribers.
func (g *Gateway) emit(e Event) {
	g.mu.Lock()
	g.nextEvID++
	e.ID = g.nextEvID
	if e.Time == 0 {
		e.Time = time.Now().UnixMilli()
	}
	g.events = append(g.events, e)
	if len(g.events) > eventBufferLen {
		g.events = g.events[len(g.events)-eventBufferLen:]
	}
	g.mu.Unlock()
	g.log.Info("cluster event", "kind", e.Kind, "message", e.Message)
	if b, err := json.Marshal(e); err == nil {
		g.hub.publish("event", b)
	}
}

func (g *Gateway) snapshot() *Snapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snap
}

func (g *Gateway) recentEvents() []Event {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]Event{}, g.events...)
}

// ---------------------------------------------------------------- SSE hub

type sseMsg struct {
	event string
	data  []byte
}

// hub fans server-sent events out to every connected browser. A slow
// subscriber drops messages rather than blocking the collector.
type hub struct {
	mu   sync.Mutex
	subs map[chan sseMsg]struct{}
}

func newHub() *hub { return &hub{subs: map[chan sseMsg]struct{}{}} }

func (h *hub) subscribe() chan sseMsg {
	ch := make(chan sseMsg, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan sseMsg) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *hub) publish(event string, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- sseMsg{event, data}:
		default:
		}
	}
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// ---------------------------------------------------------------- ack log

// ackLog remembers how recent dashboard-produced records were acknowledged,
// so the live stream can show the acks mode for them.
type ackLog struct {
	mu    sync.Mutex
	m     map[string]string
	order []string
	cap   int
}

func newAckLog(n int) *ackLog { return &ackLog{m: map[string]string{}, cap: n} }

func (a *ackLog) put(id, desc string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.m[id]; !ok {
		a.order = append(a.order, id)
	}
	a.m[id] = desc
	if len(a.order) > a.cap {
		delete(a.m, a.order[0])
		a.order = a.order[1:]
	}
}

func (a *ackLog) get(id string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, ok := a.m[id]
	return d, ok
}

// SeedDemo prepares a demo workload through the normal APIs: it creates
// the topic (if missing), starts demo consumers in a group and starts the
// traffic generator. Used by `streamhub dashboard --managed --demo`.
func (g *Gateway) SeedDemo(ctx context.Context, topic, group string, consumers, rate int, failureRate float64) error {
	// Wait for a working cluster (controller elected, all brokers in).
	for {
		if s := g.snapshot(); s != nil && s.Reachable && s.ControllerID >= 0 && s.Totals.HealthyBrokers == s.Totals.Brokers && s.Totals.Brokers > 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	rf := int16(min(3, len(g.snapshot().Brokers)))
	if _, err := g.client.Topic(ctx, topic); err != nil {
		if err := g.client.CreateTopic(ctx, client.TopicSpec{Name: topic, Partitions: 6, ReplicationFactor: rf}); err != nil {
			return err
		}
	}
	for i := 0; i < consumers; i++ {
		if _, err := g.demo.addConsumer(AddConsumerRequest{Group: group, Topic: topic, Strategy: "range"}); err != nil {
			return err
		}
	}
	if rate > 0 {
		g.demo.setTraffic(TrafficView{Running: true, Topic: topic, Rate: rate, FailureRate: failureRate})
		g.emit(Event{Kind: "traffic", Severity: "info", Message: fmt.Sprintf("Demo traffic started: %d msg/s to %s", rate, topic)})
	}
	return nil
}
