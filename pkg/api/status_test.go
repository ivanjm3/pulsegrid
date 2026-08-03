package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"pulsegrid/pkg"
)

func decodeJSON(t *testing.T, body []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode json: %v (body: %s)", err, body)
	}
}

// fakeJobGetter is an in-memory stand-in for JobGetter.
type fakeJobGetter struct {
	jobs map[string]pkg.Job
}

func newFakeJobGetter() *fakeJobGetter {
	return &fakeJobGetter{jobs: make(map[string]pkg.Job)}
}

func (f *fakeJobGetter) GetJob(ctx context.Context, jobID string) (pkg.Job, error) {
	job, ok := f.jobs[jobID]
	if !ok {
		return pkg.Job{}, pgx.ErrNoRows
	}
	return job, nil
}

// fakeManifestFetcher is an in-memory stand-in for ManifestFetcher.
type fakeManifestFetcher struct {
	manifests map[string]pkg.Manifest
	err       error
}

func newFakeManifestFetcher() *fakeManifestFetcher {
	return &fakeManifestFetcher{manifests: make(map[string]pkg.Manifest)}
}

func (f *fakeManifestFetcher) FetchManifest(ctx context.Context, jobID string) (pkg.Manifest, error) {
	if f.err != nil {
		return pkg.Manifest{}, f.err
	}
	m, ok := f.manifests[jobID]
	if !ok {
		return pkg.Manifest{}, errors.New("manifest not found")
	}
	return m, nil
}

func doStatusRequest(h *StatusHandler, jobID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID, nil)
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestStatusHandler_CompletedJob_ReturnsOutputFiles(t *testing.T) {
	store := newFakeJobGetter()
	completion := time.Date(2024, 1, 15, 10, 35, 0, 0, time.UTC)
	store.jobs["job-1"] = pkg.Job{
		ID:             "job-1",
		Status:         pkg.JobStatusCompleted,
		SubmissionTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		CompletionTime: &completion,
		RetryCount:     0,
	}

	manifests := newFakeManifestFetcher()
	manifests.manifests["job-1"] = pkg.Manifest{
		JobID: "job-1",
		OutputFiles: []pkg.OutputFile{
			{Rendition: "720p", Path: "s3://pulsegrid-output/job-1/720p/720p.mp4", SizeBytes: 524288000, DurationSeconds: 300},
		},
	}

	h := NewStatusHandler(store, manifests)
	rec := doStatusRequest(h, "job-1")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp StatusResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)

	if resp.Status != "completed" {
		t.Fatalf("expected status completed, got %q", resp.Status)
	}
	if resp.CompletionTime == nil {
		t.Fatal("expected non-nil completion_time")
	}
	if len(resp.OutputFiles) != 1 || resp.OutputFiles[0].Rendition != "720p" {
		t.Fatalf("expected 1 output file for 720p, got %+v", resp.OutputFiles)
	}
}

func TestStatusHandler_ProcessingJob_NoCompletionTime(t *testing.T) {
	store := newFakeJobGetter()
	store.jobs["job-2"] = pkg.Job{
		ID:             "job-2",
		Status:         pkg.JobStatusProcessing,
		SubmissionTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		RetryCount:     0,
	}

	h := NewStatusHandler(store, newFakeManifestFetcher())
	rec := doStatusRequest(h, "job-2")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp StatusResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)

	if resp.Status != "processing" {
		t.Fatalf("expected status processing, got %q", resp.Status)
	}
	if resp.CompletionTime != nil {
		t.Fatalf("expected nil completion_time, got %v", *resp.CompletionTime)
	}
	if len(resp.OutputFiles) != 0 {
		t.Fatalf("expected no output files, got %+v", resp.OutputFiles)
	}
}

func TestStatusHandler_NonexistentJob_404(t *testing.T) {
	h := NewStatusHandler(newFakeJobGetter(), newFakeManifestFetcher())
	rec := doStatusRequest(h, "does-not-exist")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStatusHandler_FailedJob_ReturnsFailureReason(t *testing.T) {
	store := newFakeJobGetter()
	reason := "unsupported codec: vp8"
	store.jobs["job-3"] = pkg.Job{
		ID:             "job-3",
		Status:         pkg.JobStatusFailed,
		SubmissionTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		RetryCount:     3,
		FailureReason:  &reason,
	}

	h := NewStatusHandler(store, newFakeManifestFetcher())
	rec := doStatusRequest(h, "job-3")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp StatusResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)

	if resp.Status != "failed" {
		t.Fatalf("expected status failed, got %q", resp.Status)
	}
	if resp.FailureReason == nil || *resp.FailureReason != reason {
		t.Fatalf("expected failure_reason %q, got %v", reason, resp.FailureReason)
	}
}

func TestStatusHandler_CompletedJob_ManifestFetchFails_DegradesGracefully(t *testing.T) {
	store := newFakeJobGetter()
	store.jobs["job-4"] = pkg.Job{
		ID:             "job-4",
		Status:         pkg.JobStatusCompleted,
		SubmissionTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	manifests := newFakeManifestFetcher()
	manifests.err = errors.New("s3: object not found")

	h := NewStatusHandler(store, manifests)
	rec := doStatusRequest(h, "job-4")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (degrade, not fail), got %d: %s", rec.Code, rec.Body.String())
	}
	var resp StatusResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)
	if len(resp.OutputFiles) != 0 {
		t.Fatalf("expected empty output files on manifest fetch failure, got %+v", resp.OutputFiles)
	}
}

func TestStatusHandler_WrongMethod_405(t *testing.T) {
	h := NewStatusHandler(newFakeJobGetter(), newFakeManifestFetcher())
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-1", nil)
	req.SetPathValue("job_id", "job-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}
