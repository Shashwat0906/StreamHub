package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metrics"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

type brokerMetrics struct {
	reg        *metrics.Registry
	requests   *metrics.CounterVec
	latency    *metrics.HistogramVec
	recordsIn  *metrics.Counter
	bytesOut   *metrics.Counter
	isrShrinks *metrics.Counter
	isrExpands *metrics.Counter
	elections  *metrics.Counter
	rebalances *metrics.Counter
}

func newBrokerMetrics() *brokerMetrics {
	reg := metrics.NewRegistry()
	return &brokerMetrics{
		reg:        reg,
		requests:   reg.CounterVec("streamhub_requests_total", "Requests handled, by API and error code.", "api", "error"),
		latency:    reg.HistogramVec("streamhub_request_duration_seconds", "Request handling latency (includes long-poll and acks=all waits).", metrics.DefaultLatencyBuckets, "api"),
		recordsIn:  reg.Counter("streamhub_records_produced_total", "Records received by Produce requests."),
		bytesOut:   reg.Counter("streamhub_fetch_bytes_total", "Record bytes returned by Fetch requests."),
		isrShrinks: reg.Counter("streamhub_isr_shrinks_total", "ISR shrink requests issued by this broker as leader."),
		isrExpands: reg.Counter("streamhub_isr_expands_total", "ISR expand requests issued by this broker as leader."),
		elections:  reg.Counter("streamhub_controller_broker_fencings_total", "Brokers fenced by this broker while controller."),
		rebalances: reg.Counter("streamhub_group_rebalances_total", "Consumer group rebalances completed on this coordinator."),
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

// startHTTP serves /metrics, /healthz and /readyz.
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
	ln, err := net.Listen("tcp", b.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", b.cfg.HTTPAddr, err)
	}
	b.httpAddr = ln.Addr().String()
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	b.closers = append(b.closers, srv.Close)
	go srv.Serve(ln)
	return nil
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
