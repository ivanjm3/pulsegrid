package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AnalyticsMetrics holds the analytics-consumer's Prometheus collectors
// (task 40).
type AnalyticsMetrics struct {
	EventsProcessedTotal *prometheus.CounterVec
	SinkLagSeconds       prometheus.Gauge
	ConsumerErrorsTotal  *prometheus.CounterVec
	registry             *prometheus.Registry
}

// NewAnalytics creates an AnalyticsMetrics instance registered against a
// fresh registry.
func NewAnalytics() *AnalyticsMetrics {
	reg := prometheus.NewRegistry()

	m := &AnalyticsMetrics{
		EventsProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsegrid_analytics_events_processed_total",
			Help: "Total lifecycle events successfully sunk, labeled by event_type",
		}, []string{"event_type"}),
		SinkLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsegrid_analytics_sink_lag_seconds",
			Help: "Most recent received_at - event_time latency, in seconds",
		}),
		ConsumerErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsegrid_analytics_consumer_errors_total",
			Help: "Total analytics consumer errors, labeled by error_type (sink_write_failure|parse_error|kafka_poll_error)",
		}, []string{"error_type"}),
		registry: reg,
	}

	reg.MustRegister(m.EventsProcessedTotal, m.SinkLagSeconds, m.ConsumerErrorsTotal)
	return m
}

// Handler returns the /metrics HTTP handler exposing all registered
// collectors in Prometheus text format.
func (m *AnalyticsMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// IncEventsProcessed increments the processed-events counter for the given
// event_type label.
func (m *AnalyticsMetrics) IncEventsProcessed(eventType string) {
	m.EventsProcessedTotal.WithLabelValues(eventType).Inc()
}

// SetSinkLag records the most recent received_at - event_time latency.
func (m *AnalyticsMetrics) SetSinkLag(seconds float64) {
	m.SinkLagSeconds.Set(seconds)
}

// IncConsumerError increments the consumer error counter for the given
// error_type label.
func (m *AnalyticsMetrics) IncConsumerError(errorType string) {
	m.ConsumerErrorsTotal.WithLabelValues(errorType).Inc()
}
