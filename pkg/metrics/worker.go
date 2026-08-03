package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// WorkerMetrics holds the worker pod's Prometheus collectors for job
// completion, transcode failure classification (task 18), transcode
// duration, and resource-constraint tracking (task 21).
type WorkerMetrics struct {
	JobCompletedTotal        prometheus.Counter
	TranscodeFailureTotal    *prometheus.CounterVec
	TranscodeDurationSeconds *prometheus.HistogramVec
	PodResourceConstrained   prometheus.Counter
	JobsDLQTotal             prometheus.Counter
	registry                 *prometheus.Registry
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
		TranscodeDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pulsegrid_transcode_duration_seconds",
			Help:    "Wall-clock ffmpeg transcode duration per rendition",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s .. ~34min
		}, []string{"rendition"}),
		PodResourceConstrained: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsegrid_pod_resource_constrained",
			Help: "Total times this pod hit a fatal resource constraint (disk, OOM)",
		}),
		JobsDLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsegrid_jobs_dlq_total",
			Help: "Total jobs moved to the dead letter queue (permanent error or max retries exceeded)",
		}),
		registry: reg,
	}

	reg.MustRegister(m.JobCompletedTotal, m.TranscodeFailureTotal, m.TranscodeDurationSeconds, m.PodResourceConstrained, m.JobsDLQTotal)
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

// ObserveTranscodeDuration records how long a single rendition's ffmpeg
// invocation took, labeled by rendition ID.
func (m *WorkerMetrics) ObserveTranscodeDuration(rendition string, seconds float64) {
	m.TranscodeDurationSeconds.WithLabelValues(rendition).Observe(seconds)
}

// IncPodResourceConstrained increments the counter tracking fatal resource
// constraints (disk, OOM) hit by this pod.
func (m *WorkerMetrics) IncPodResourceConstrained() {
	m.PodResourceConstrained.Inc()
}

// IncJobsDLQ increments the counter tracking jobs moved to the dead letter
// queue, per Requirement 8.5.
func (m *WorkerMetrics) IncJobsDLQ() {
	m.JobsDLQTotal.Inc()
}
