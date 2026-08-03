// Package metrics defines the Pulsegrid API server's Prometheus metrics and
// the /metrics HTTP handler that exposes them.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the API server's Prometheus collectors.
type Metrics struct {
	JobsSubmittedTotal prometheus.Counter
	UploadDurationSecs prometheus.Histogram
	QueueDepthJobs     prometheus.Gauge
	registry           *prometheus.Registry
}

// New creates a Metrics instance registered against a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		JobsSubmittedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsegrid_jobs_submitted_total",
			Help: "Total jobs submitted",
		}),
		UploadDurationSecs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsegrid_upload_duration_seconds",
			Help:    "Upload duration in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		QueueDepthJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsegrid_queue_depth_jobs",
			Help: "Current queue depth",
		}),
		registry: reg,
	}

	reg.MustRegister(m.JobsSubmittedTotal, m.UploadDurationSecs, m.QueueDepthJobs)
	return m
}

// Handler returns the /metrics HTTP handler exposing all registered
// collectors in OpenMetrics/Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// IncJobsSubmitted increments the total submitted jobs counter.
func (m *Metrics) IncJobsSubmitted() {
	m.JobsSubmittedTotal.Inc()
}

// ObserveUploadDuration records one upload's duration in seconds.
func (m *Metrics) ObserveUploadDuration(seconds float64) {
	m.UploadDurationSecs.Observe(seconds)
}

// SetQueueDepth sets the current queue depth gauge.
func (m *Metrics) SetQueueDepth(depth float64) {
	m.QueueDepthJobs.Set(depth)
}
