package metrics

import (
	"strings"
	"testing"
)

func TestPrometheusText(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "help x")
	c.Add(3)
	v := r.CounterVec("req_total", "reqs", "api")
	v.With("Produce").Add(2)
	h := r.HistogramVec("lat_seconds", "lat", []float64{0.1, 1}, "api")
	h.Observe(0.05, "Fetch")
	h.Observe(0.5, "Fetch")
	h.Observe(5, "Fetch")
	r.GaugeFunc("g", "gauge", func() []Sample { return []Sample{{Labels: map[string]string{"p": "0"}, Value: 1.5}} })
	var sb strings.Builder
	r.WritePrometheus(&sb)
	out := sb.String()
	for _, want := range []string{
		"# TYPE x_total counter", "x_total 3",
		`req_total{api="Produce"} 2`,
		`lat_seconds_bucket{api="Fetch",le="0.1"} 1`,
		`lat_seconds_bucket{api="Fetch",le="1"} 2`,
		`lat_seconds_bucket{api="Fetch",le="+Inf"} 3`,
		`lat_seconds_count{api="Fetch"} 3`,
		`g{p="0"} 1.5`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}
