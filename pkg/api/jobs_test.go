package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulsegrid/pkg"
	"pulsegrid/pkg/store"
)

// fakeJobLister is a test double for JobLister.
type fakeJobLister struct {
	jobs      []store.JobSummary
	total     int
	err       error
	lastQuery store.JobFilter
}

func (f *fakeJobLister) ListJobs(ctx context.Context, filter store.JobFilter) ([]store.JobSummary, int, error) {
	f.lastQuery = filter
	if f.err != nil {
		return nil, 0, f.err
	}
	return f.jobs, f.total, nil
}

func TestJobsListHandler_ReturnsFilteredResults(t *testing.T) {
	completion := time.Date(2024, 1, 15, 10, 35, 15, 0, time.UTC)
	submission := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lister := &fakeJobLister{
		jobs: []store.JobSummary{
			{ID: "job-1", Status: pkg.JobStatusCompleted, SubmissionTime: submission, CompletionTime: &completion},
		},
		total: 1,
	}
	h := NewJobsListHandler(lister)

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=2024-01-15T00:00:00Z&submitted_before=2024-01-15T12:00:00Z&status=completed&limit=50&offset=0", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp JobsListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Total != 1 || len(resp.Jobs) != 1 {
		t.Fatalf("got total=%d jobs=%d, want total=1 jobs=1", resp.Total, len(resp.Jobs))
	}
	if resp.Jobs[0].JobID != "job-1" {
		t.Errorf("job_id = %q, want job-1", resp.Jobs[0].JobID)
	}
	if resp.Jobs[0].CompletionTime == nil {
		t.Fatal("expected completion_time to be set")
	}
	if resp.Jobs[0].DurationSeconds == nil || *resp.Jobs[0].DurationSeconds != 315 {
		t.Errorf("duration_seconds = %v, want 315", resp.Jobs[0].DurationSeconds)
	}
	if resp.Limit != 50 || resp.Offset != 0 {
		t.Errorf("limit=%d offset=%d, want limit=50 offset=0", resp.Limit, resp.Offset)
	}

	if len(lister.lastQuery.Statuses) != 1 || lister.lastQuery.Statuses[0] != pkg.JobStatusCompleted {
		t.Errorf("expected status filter [completed], got %v", lister.lastQuery.Statuses)
	}
	if lister.lastQuery.SubmittedAfter == nil || lister.lastQuery.SubmittedBefore == nil {
		t.Error("expected submitted_after/submitted_before to be parsed")
	}
}

func TestJobsListHandler_DefaultsLimitAndOffset(t *testing.T) {
	lister := &fakeJobLister{}
	h := NewJobsListHandler(lister)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if lister.lastQuery.Limit != DefaultListLimit {
		t.Errorf("limit = %d, want %d", lister.lastQuery.Limit, DefaultListLimit)
	}
	if lister.lastQuery.Offset != 0 {
		t.Errorf("offset = %d, want 0", lister.lastQuery.Offset)
	}
}

func TestJobsListHandler_LimitExceedsMax_Returns400(t *testing.T) {
	h := NewJobsListHandler(&fakeJobLister{})

	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=1001", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestJobsListHandler_InvalidTimestamp_Returns400(t *testing.T) {
	h := NewJobsListHandler(&fakeJobLister{})

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=not-a-timestamp", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestJobsListHandler_InvalidStatus_Returns400(t *testing.T) {
	h := NewJobsListHandler(&fakeJobLister{})

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=bogus", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestJobsListHandler_AfterAfterBefore_Returns400(t *testing.T) {
	h := NewJobsListHandler(&fakeJobLister{})

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=2024-01-15T12:00:00Z&submitted_before=2024-01-15T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestJobsListHandler_WrongMethod_Returns405(t *testing.T) {
	h := NewJobsListHandler(&fakeJobLister{})

	req := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestJobsListHandler_StoreError_Returns500(t *testing.T) {
	lister := &fakeJobLister{err: context.DeadlineExceeded}
	h := NewJobsListHandler(lister)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
