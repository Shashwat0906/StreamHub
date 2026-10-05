// Package metrics is a tiny Prometheus-compatible metrics registry
// (counters, gauges, histograms with optional labels) rendered in the
// Prometheus text exposition format. It exists so StreamHub has no
// third-party dependencies.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families.
type Registry struct {
	mu       sync.Mutex
	families []family
	gaugeFns []gaugeFn
}

type family interface {
	write(w io.Writer)
	name() string
}

type gaugeFn struct {
	name, help string
	fn         func() []Sample
}

// Sample is one labelled value produced by a gauge function.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// GaugeFunc registers a gauge computed at scrape time.
func (r *Registry) GaugeFunc(name, help string, fn func() []Sample) {
	r.mu.Lock()
	r.gaugeFns = append(r.gaugeFns, gaugeFn{name, help, fn})
	r.mu.Unlock()
}

// WritePrometheus renders every metric.
func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	fns := append([]gaugeFn(nil), r.gaugeFns...)
	r.mu.Unlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name() < fams[j].name() })
	for _, f := range fams {
		f.write(w)
	}
	for _, g := range fns {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
		for _, s := range g.fn() {
			fmt.Fprintf(w, "%s%s %s\n", g.name, formatLabels(s.Labels), formatFloat(s.Value))
		}
	}
}

// ---------------------------------------------------------------- atomic float

type atomicFloat struct{ bits atomic.Uint64 }

func (a *atomicFloat) Add(v float64) {
	for {
		old := a.bits.Load()
		nv := math.Float64bits(math.Float64frombits(old) + v)
		if a.bits.CompareAndSwap(old, nv) {
			return
		}
	}
}
func (a *atomicFloat) Set(v float64) { a.bits.Store(math.Float64bits(v)) }
func (a *atomicFloat) Load() float64 { return math.Float64frombits(a.bits.Load()) }

// ---------------------------------------------------------------- counter / gauge

// Counter is a monotonically increasing value.
type Counter struct {
	fname, help, typ string
	v                atomicFloat
}

func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{fname: name, help: help, typ: "counter"}
	r.add(c)
	return c
}

// Gauge is a value that can go up and down.
func (r *Registry) Gauge(name, help string) *Counter {
	c := &Counter{fname: name, help: help, typ: "gauge"}
	r.add(c)
	return c
}

func (c *Counter) Add(v float64)  { c.v.Add(v) }
func (c *Counter) Inc()           { c.v.Add(1) }
func (c *Counter) Set(v float64)  { c.v.Set(v) }
func (c *Counter) Value() float64 { return c.v.Load() }
func (c *Counter) name() string   { return c.fname }
func (c *Counter) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", c.fname, c.help, c.fname, c.typ, c.fname, formatFloat(c.v.Load()))
}

// CounterVec is a counter with labels.
type CounterVec struct {
	fname, help string
	labels      []string
	mu          sync.Mutex
	children    map[string]*labelled
}

type labelled struct {
	values []string
	v      atomicFloat
}

func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{fname: name, help: help, labels: labels, children: map[string]*labelled{}}
	r.add(c)
	return c
}

// With returns the child for label values (in declaration order).
func (c *CounterVec) With(values ...string) *atomicFloat {
	key := strings.Join(values, "\xff")
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.children[key]
	if !ok {
		ch = &labelled{values: values}
		c.children[key] = ch
	}
	return &ch.v
}

func (c *CounterVec) name() string { return c.fname }
func (c *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.fname, c.help, c.fname)
	c.mu.Lock()
	keys := make([]string, 0, len(c.children))
	for k := range c.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ch := c.children[k]
		fmt.Fprintf(w, "%s%s %s\n", c.fname, formatLabels(zip(c.labels, ch.values)), formatFloat(ch.v.Load()))
	}
	c.mu.Unlock()
}

// ---------------------------------------------------------------- histogram

// HistogramVec is a labelled histogram with fixed buckets.
type HistogramVec struct {
	fname, help string
	labels      []string
	buckets     []float64
	mu          sync.Mutex
	children    map[string]*histChild
}

type histChild struct {
	values []string
	counts []uint64 // per bucket (non-cumulative), +Inf last
	sum    float64
	count  uint64
}

// DefaultLatencyBuckets in seconds.
var DefaultLatencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func (r *Registry) HistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	h := &HistogramVec{fname: name, help: help, labels: labels, buckets: buckets, children: map[string]*histChild{}}
	r.add(h)
	return h
}

// Observe records v for the given label values.
func (h *HistogramVec) Observe(v float64, values ...string) {
	key := strings.Join(values, "\xff")
	h.mu.Lock()
	defer h.mu.Unlock()
	ch, ok := h.children[key]
	if !ok {
		ch = &histChild{values: values, counts: make([]uint64, len(h.buckets)+1)}
		h.children[key] = ch
	}
	i := sort.SearchFloat64s(h.buckets, v)
	ch.counts[i]++
	ch.sum += v
	ch.count++
}

func (h *HistogramVec) name() string { return h.fname }
func (h *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.fname, h.help, h.fname)
	h.mu.Lock()
	defer h.mu.Unlock()
	keys := make([]string, 0, len(h.children))
	for k := range h.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ch := h.children[k]
		base := zip(h.labels, ch.values)
		var cum uint64
		for i, ub := range h.buckets {
			cum += ch.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.fname, formatLabels(withLabel(base, "le", formatFloat(ub))), cum)
		}
		cum += ch.counts[len(h.buckets)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.fname, formatLabels(withLabel(base, "le", "+Inf")), cum)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.fname, formatLabels(base), formatFloat(ch.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.fname, formatLabels(base), ch.count)
	}
}

// ---------------------------------------------------------------- helpers

func zip(keys, values []string) map[string]string {
	m := make(map[string]string, len(keys))
	for i, k := range keys {
		if i < len(values) {
			m[k] = values[i]
		}
	}
	return m
}

func withLabel(m map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(m)+1)
	for a, b := range m {
		out[a] = b
	}
	out[k] = v
	return out
}

func formatLabels(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(m[k])
		parts[i] = fmt.Sprintf(`%s="%s"`, k, v)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
