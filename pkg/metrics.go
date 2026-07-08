package pkg

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for the API server.
type Metrics struct {
	JobsSubmittedTotal      prometheus.Counter
	UploadDurationSeconds   prometheus.Histogram
	QueueDepthJobs          prometheus.Gauge
}

// DefaultHistogramBuckets defines the upload duration histogram buckets.
// Covers sub-second uploads to multi-minute large file uploads.
var DefaultHistogramBuckets = []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0}

// NewMetrics creates and registers Prometheus metrics with the given registry.
// If registry is nil, uses prometheus.DefaultRegisterer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{
		JobsSubmittedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsegrid_jobs_submitted_total",
			Help: "Total number of jobs submitted to the transcoding pipeline",
		}),
		UploadDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsegrid_upload_duration_seconds",
			Help:    "Duration of upload processing in seconds (parse, validate, S3 upload, enqueue)",
			Buckets: DefaultHistogramBuckets,
		}),
		QueueDepthJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsegrid_queue_depth_jobs",
			Help: "Current number of jobs waiting in the transcoding-jobs Kafka topic (sum of partition lags)",
		}),
	}

	reg.MustRegister(m.JobsSubmittedTotal)
	reg.MustRegister(m.UploadDurationSeconds)
	reg.MustRegister(m.QueueDepthJobs)

	return m
}
