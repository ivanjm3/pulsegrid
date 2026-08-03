package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// WorkerMetrics holds the worker pod's Prometheus collectors for job
// completion and transcode failure classification (task 18). Duration
// histograms and resource-constraint gauges are added by task 21.
type WorkerMetrics struct {
	JobCompletedTotal     prometheus.Counter
	TranscodeFailureTotal *prometheus.CounterVec
	registry              *prometheus.Registry
}

// NewWorker creates a WorkerMetrics instance registered against a fresh
// registry.
func NewWorker() *WorkerMetrics {
	reg := prometheus.NewRegistry()

	m := &WorkerMetrics{
		JobCompletedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsegrid_job_completed_total",
			Help: "Total jobs completed successfully",
		}),
		TranscodeFailureTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsegrid_transcode_failure",
			Help: "Total transcode failures, labeled by error_type (retryable|permanent|constraint)",
		}, []string{"error_type"}),
		registry: reg,
	}

	reg.MustRegister(m.JobCompletedTotal, m.TranscodeFailureTotal)
	return m
}

// Handler returns the /metrics HTTP handler exposing all registered
// collectors in Prometheus text format.
func (m *WorkerMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// IncJobCompleted increments the total completed jobs counter.
func (m *WorkerMetrics) IncJobCompleted() {
	m.JobCompletedTotal.Inc()
}

// IncTranscodeFailure increments the transcode failure counter for the given
// error_type label.
func (m *WorkerMetrics) IncTranscodeFailure(errorType string) {
	m.TranscodeFailureTotal.WithLabelValues(errorType).Inc()
}
