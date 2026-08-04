package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAnalyticsMetrics_IncEventsProcessed_LabelsByEventType(t *testing.T) {
	m := NewAnalytics()

	m.IncEventsProcessed("job_started")
	m.IncEventsProcessed("job_started")
	m.IncEventsProcessed("rendition_completed")

	if got := testutil.ToFloat64(m.EventsProcessedTotal.WithLabelValues("job_started")); got != 2 {
		t.Errorf("job_started count = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.EventsProcessedTotal.WithLabelValues("rendition_completed")); got != 1 {
		t.Errorf("rendition_completed count = %v, want 1", got)
	}
}

func TestAnalyticsMetrics_SetSinkLag_SetsGauge(t *testing.T) {
	m := NewAnalytics()

	m.SetSinkLag(1.5)

	if got := testutil.ToFloat64(m.SinkLagSeconds); got != 1.5 {
		t.Errorf("sink lag = %v, want 1.5", got)
	}
}

func TestAnalyticsMetrics_IncConsumerError_LabelsByErrorType(t *testing.T) {
	m := NewAnalytics()

	m.IncConsumerError("sink_write_failure")
	m.IncConsumerError("parse_error")
	m.IncConsumerError("parse_error")
	m.IncConsumerError("kafka_poll_error")

	for errType, want := range map[string]float64{
		"sink_write_failure": 1,
		"parse_error":        2,
		"kafka_poll_error":   1,
	} {
		if got := testutil.ToFloat64(m.ConsumerErrorsTotal.WithLabelValues(errType)); got != want {
			t.Errorf("%s count = %v, want %v", errType, got, want)
		}
	}
}
