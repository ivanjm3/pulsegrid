package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestIncJobsSubmitted_IncrementsCounter(t *testing.T) {
	m := New()

	m.IncJobsSubmitted()
	m.IncJobsSubmitted()
	m.IncJobsSubmitted()

	if got := testutil.ToFloat64(m.JobsSubmittedTotal); got != 3 {
		t.Errorf("pulsegrid_jobs_submitted_total = %v, want 3", got)
	}
}

func TestSetQueueDepth_SetsGauge(t *testing.T) {
	m := New()

	m.SetQueueDepth(147)

	if got := testutil.ToFloat64(m.QueueDepthJobs); got != 147 {
		t.Errorf("pulsegrid_queue_depth_jobs = %v, want 147", got)
	}
}

func TestObserveUploadDuration_RecordsIntoCorrectBuckets(t *testing.T) {
	m := New()

	// prometheus.DefBuckets includes 0.5 and 5; a 0.3s upload should land in
	// the <=0.5 bucket (and every larger bucket) but not the <=0.25 bucket.
	m.ObserveUploadDuration(0.3)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `pulsegrid_upload_duration_seconds_bucket{le="0.5"} 1`) {
		t.Errorf("expected le=0.5 bucket to contain the observation, body:\n%s", body)
	}
	if !strings.Contains(body, `pulsegrid_upload_duration_seconds_bucket{le="0.25"} 0`) {
		t.Errorf("expected le=0.25 bucket to be empty, body:\n%s", body)
	}
	if !strings.Contains(body, `pulsegrid_upload_duration_seconds_bucket{le="+Inf"} 1`) {
		t.Errorf("expected le=+Inf bucket to contain the observation, body:\n%s", body)
	}
	if !strings.Contains(body, "pulsegrid_upload_duration_seconds_sum 0.3") {
		t.Errorf("expected sum to reflect the observation, body:\n%s", body)
	}
}

func TestHandler_ExposesAllRegisteredMetrics(t *testing.T) {
	m := New()
	m.IncJobsSubmitted()
	m.SetQueueDepth(5)
	m.ObserveUploadDuration(1.0)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"pulsegrid_jobs_submitted_total",
		"pulsegrid_upload_duration_seconds",
		"pulsegrid_queue_depth_jobs",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("expected /metrics body to contain %q, body:\n%s", name, body)
		}
	}
}
