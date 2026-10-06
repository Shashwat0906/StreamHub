package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metrics"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

type brokerMetrics struct {
	reg           *metrics.Registry
	requests      *metrics.CounterVec
	latency       *metrics.HistogramVec
	recordsIn     *metrics.Counter
	recordsOut    *metrics.Counter
	bytesOut      *metrics.Counter
	produceErrors *metrics.Counter
	isrShrinks    *metrics.Counter
	isrExpands    *metrics.Counter
	elections     *metrics.Counter
	rebalances    *metrics.Counter
}

func newBrokerMetrics() *brokerMetrics {
	reg := metrics.NewRegistry()
	return &brokerMetrics{
		reg:           reg,
		requests:      reg.CounterVec("streamhub_requests_total", "Requests handled, by API and error code.", "api", "error"),
		latency:       reg.HistogramVec("streamhub_request_duration_seconds", "Request handling latency (includes long-poll and acks=all waits).", metrics.DefaultLatencyBuckets, "api"),
		recordsIn:     reg.Counter("streamhub_records_produced_total", "Records received by Produce requests."),
		bytesOut:      reg.Counter("streamhub_fetch_bytes_total", "Record bytes returned by Fetch requests."),
		recordsOut:    reg.Counter("streamhub_records_consumed_total", "Records returned to consumers (follower fetches excluded)."),
		produceErrors: reg.Counter("streamhub_produce_errors_total", "Produce requests answered with an error."),
		isrShrinks:    reg.Counter("streamhub_isr_shrinks_total", "ISR shrink requests issued by this broker as leader."),
		isrExpands:    reg.Counter("streamhub_isr_expands_total", "ISR expand requests issued by this broker as leader."),
		elections:     reg.Counter("streamhub_controller_broker_fencings_total", "Brokers fenced by this broker while controller."),
		rebalances:    reg.Counter("streamhub_group_rebalances_total", "Consumer group rebalances completed on this coordinator."),
	}
}

// responseError extracts the error code from any response for metrics.
func responseError(m protocol.Message) protocol.ErrorCode {
	switch r := m.(type) {
	case nil:
		return protocol.ErrNone
	case *protocol.ProduceResponse:
		return r.Err
	case *protocol.FetchResponse:
		if r.Err != protocol.ErrNone {
			return r.Err
		}
		for _, p := range r.Partitions {
			if p.Err != protocol.ErrNone {
				return p.Err
			}
		}
	case *protocol.SimpleResponse:
		return r.Err
	case *protocol.MetadataResponse:
		return r.Err
	case *protocol.ListOffsetsResponse:
		return r.Err
	case *protocol.JoinGroupResponse:
		return r.Err
	case *protocol.OffsetFetchResponse:
		return r.Err
	case *protocol.FindCoordinatorResponse:
		return r.Err
	case *protocol.BrokerHeartbeatResponse:
		return r.Err
	case *protocol.DescribeGroupResponse:
		return r.Err
	case *protocol.OffsetForLeaderEpochResponse:
		return r.Err
	case *protocol.InitProducerIDResponse:
		return r.Err
	}
	return protocol.ErrNone
}

func (b *Broker) observeRequest(api protocol.APIKey, d time.Duration, resp protocol.Message) {
	if b.metrics == nil {
		return
	}
	b.metrics.requests.With(api.String(), responseError(resp).String()).Add(1)
	b.metrics.latency.Observe(d.Seconds(), api.String())
}

func (b *Broker) registerGauges() {
	reg := b.metrics.reg
	reg.GaugeFunc("streamhub_partition_log_end_offset", "Log end offset per hosted replica.", func() []metrics.Sample {
		var out []metrics.Sample
		for _, p := range b.replicas.all() {
			out = append(out, metrics.Sample{Labels: p.labels(), Value: float64(p.log.EndOffset())})
		}
		return out
	})
	reg.GaugeFunc("streamhub_partition_high_watermark", "High-watermark per hosted replica.", func() []metrics.Sample {
		var out []metrics.Sample
		for _, p := range b.replicas.all() {
			out = append(out, metrics.Sample{Labels: p.labels(), Value: float64(p.HighWatermark())})
		}
		return out
	})
	reg.GaugeFunc("streamhub_partition_size_bytes", "On-disk log size per hosted replica.", func() []metrics.Sample {
		var out []metrics.Sample
		for _, p := range b.replicas.all() {
			out = append(out, metrics.Sample{Labels: p.labels(), Value: float64(p.log.Size())})
		}
		return out
	})
	reg.GaugeFunc("streamhub_partition_isr_size", "ISR size for partitions this broker leads.", func() []metrics.Sample {
		var out []metrics.Sample
		for _, p := range b.replicas.all() {
			r, _, _, _, isr := p.Snapshot()
			if r == roleLeader {
				out = append(out, metrics.Sample{Labels: p.labels(), Value: float64(len(isr))})
			}
		}
		return out
	})
	reg.GaugeFunc("streamhub_partition_under_replicated", "1 if a led partition has ISR smaller than its replica set.", func() []metrics.Sample {
		var out []metrics.Sample
		for _, p := range b.replicas.all() {
			p.mu.Lock()
			isLeader, under := p.role == roleLeader, len(p.isr) < len(p.replicas)
			p.mu.Unlock()
			if isLeader {
				v := 0.0
				if under {
					v = 1
				}
				out = append(out, metrics.Sample{Labels: p.labels(), Value: v})
			}
		}
		return out
	})
	reg.GaugeFunc("streamhub_is_controller", "1 if this broker is the active controller.", func() []metrics.Sample {
		v := 0.0
		if b.proposer != nil && b.proposer.IsLeader() {
			v = 1
		}
		return []metrics.Sample{{Value: v}}
	})
	reg.GaugeFunc("streamhub_leader_partitions", "Number of partitions this broker leads.", func() []metrics.Sample {
		n := 0
		for _, p := range b.replicas.all() {
			if r, _, _, _, _ := p.Snapshot(); r == roleLeader {
				n++
			}
		}
		return []metrics.Sample{{Value: float64(n)}}
	})
}

func (p *Partition) labels() map[string]string {
	return map[string]string{"topic": p.Topic, "partition": strconv.Itoa(int(p.ID))}
}

// startHTTP serves /metrics, /healthz, /readyz and /v1/state.
//
// It is started before the rest of the broker so the advertised HTTP
// address is known when the broker registers; handlers wait for
// initialisation to finish before touching broker state.
func (b *Broker) startHTTP() error {
	if b.cfg.HTTPAddr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		b.metrics.reg.WritePrometheus(w)
	})
	// /healthz: the process is up and serving.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b.health())
	})
	// /readyz: the broker is part of a working cluster (knows a controller
	// and is not fenced). 503 otherwise.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		h := b.health()
		w.Header().Set("Content-Type", "application/json")
		if !h.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(h)
	})
	// /v1/state: machine-readable broker state (used by the dashboard).
	mux.HandleFunc("/v1/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b.State())
	})
	ln, err := net.Listen("tcp", b.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", b.cfg.HTTPAddr, err)
	}
	b.httpAddr = ln.Addr().String()
	b.advertisedHTTP = b.cfg.AdvertisedHTTPAddr
	if b.advertisedHTTP == "" {
		b.advertisedHTTP = advertisable(b.httpAddr, b.addr)
	}
	gated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-b.initDone:
		case <-r.Context().Done():
			return
		}
		mux.ServeHTTP(w, r)
	})
	srv := &http.Server{Handler: gated, ReadHeaderTimeout: 5 * time.Second}
	b.closers = append(b.closers, srv.Close)
	go srv.Serve(ln)
	return nil
}

// advertisable turns a listen address like "[::]:8080" or "0.0.0.0:8080"
// into one others can dial, borrowing the host of the protocol address.
func advertisable(listen, protocolAddr string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsUnspecified() {
		return listen
	}
	if ph, _, err := net.SplitHostPort(protocolAddr); err == nil && ph != "" {
		if ip := net.ParseIP(ph); ip == nil || !ip.IsUnspecified() {
			return net.JoinHostPort(ph, port)
		}
	}
	return net.JoinHostPort("127.0.0.1", port)
}

// BrokerState is the /v1/state payload.
type BrokerState struct {
	BrokerID      int32            `json:"broker_id"`
	Addr          string           `json:"addr"`
	HTTPAddr      string           `json:"http_addr"`
	ControllerID  int32            `json:"controller_id"`
	IsController  bool             `json:"is_controller"`
	Fenced        bool             `json:"fenced"`
	Ready         bool             `json:"ready"`
	UptimeSeconds float64          `json:"uptime_seconds"`
	Raft          *RaftState       `json:"raft,omitempty"`
	Counters      map[string]int64 `json:"counters"`
	Partitions    []PartitionState `json:"partitions"`
	StorageBytes  int64            `json:"storage_bytes"`
	CollectedAtMs int64            `json:"collected_at_ms"`
}

// RaftState summarises this broker's metadata-quorum role.
type RaftState struct {
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	LeaderID    int32  `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
}

// PartitionState is one hosted replica.
type PartitionState struct {
	Topic       string  `json:"topic"`
	Partition   int32   `json:"partition"`
	Role        string  `json:"role"`
	Leader      int32   `json:"leader"`
	LeaderEpoch int32   `json:"leader_epoch"`
	ISR         []int32 `json:"isr"`
	LogStart    int64   `json:"log_start"`
	LogEnd      int64   `json:"log_end"`
	HighWater   int64   `json:"high_watermark"`
	SizeBytes   int64   `json:"size_bytes"`
}

// State returns a snapshot of the broker for /v1/state.
func (b *Broker) State() BrokerState {
	h := b.health()
	st := BrokerState{
		BrokerID: b.cfg.ID, Addr: b.addr, HTTPAddr: b.advertisedHTTP,
		ControllerID: h.ControllerID, IsController: h.IsController, Fenced: h.Fenced, Ready: h.Ready,
		UptimeSeconds: time.Since(b.started).Seconds(), CollectedAtMs: time.Now().UnixMilli(),
		Counters: map[string]int64{
			"records_produced": int64(b.metrics.recordsIn.Value()),
			"records_consumed": int64(b.metrics.recordsOut.Value()),
			"bytes_consumed":   int64(b.metrics.bytesOut.Value()),
			"produce_errors":   int64(b.metrics.produceErrors.Value()),
			"isr_shrinks":      int64(b.metrics.isrShrinks.Value()),
			"isr_expands":      int64(b.metrics.isrExpands.Value()),
			"brokers_fenced":   int64(b.metrics.elections.Value()),
			"group_rebalances": int64(b.metrics.rebalances.Value()),
		},
	}
	if b.raft != nil {
		rs := b.raft.Status()
		st.Raft = &RaftState{Role: rs.Role, Term: rs.Term, LeaderID: rs.LeaderID, CommitIndex: rs.CommitIndex, LastApplied: rs.LastApplied}
	}
	for _, p := range b.replicas.all() {
		r, leader, epoch, hw, isr := p.Snapshot()
		ps := PartitionState{
			Topic: p.Topic, Partition: p.ID, Role: r.String(), Leader: leader, LeaderEpoch: epoch, ISR: isr,
			LogStart: p.log.StartOffset(), LogEnd: p.log.EndOffset(), HighWater: hw, SizeBytes: p.log.Size(),
		}
		st.StorageBytes += ps.SizeBytes
		st.Partitions = append(st.Partitions, ps)
	}
	sort.Slice(st.Partitions, func(i, j int) bool {
		if st.Partitions[i].Topic != st.Partitions[j].Topic {
			return st.Partitions[i].Topic < st.Partitions[j].Topic
		}
		return st.Partitions[i].Partition < st.Partitions[j].Partition
	})
	return st
}

// Health is the /healthz payload.
type Health struct {
	BrokerID     int32  `json:"broker_id"`
	Status       string `json:"status"`
	Ready        bool   `json:"ready"`
	ControllerID int32  `json:"controller_id"`
	IsController bool   `json:"is_controller"`
	Fenced       bool   `json:"fenced"`
	Partitions   int    `json:"partitions"`
	Leaders      int    `json:"leaders"`
}

func (b *Broker) health() Health {
	h := Health{BrokerID: b.cfg.ID, Status: "ok", ControllerID: -1}
	if b.proposer != nil {
		h.ControllerID = b.proposer.LeaderID()
		h.IsController = b.proposer.IsLeader()
	}
	h.Fenced = b.isFenced()
	for _, p := range b.replicas.all() {
		h.Partitions++
		if r, _, _, _, _ := p.Snapshot(); r == roleLeader {
			h.Leaders++
		}
	}
	h.Ready = h.ControllerID >= 0 && !h.Fenced
	return h
}

// HTTPAddr returns the actual HTTP listen address ("" if disabled).
func (b *Broker) HTTPAddr() string { return b.httpAddr }

// Ready reports whether the broker knows a controller, has replayed the
// metadata log and is allowed to lead partitions.
func (b *Broker) Ready() bool {
	return b.health().Ready && b.metadataReady()
}

// RaftStatus is exposed for tests and diagnostics (zero value standalone).
func (b *Broker) IsController() bool { return b.proposer != nil && b.proposer.IsLeader() }
