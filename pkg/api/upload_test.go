package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulsegrid/pkg"
)

// fakeUploader is a test double for SourceUploader.
type fakeUploader struct {
	uploadFn func(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error)
	calls    int
}

func (f *fakeUploader) UploadSource(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error) {
	f.calls++
	if f.uploadFn != nil {
		return f.uploadFn(ctx, jobID, sourceName, body)
	}
	return fmt.Sprintf("s3://pulsegrid-source/%s/original.mp4", jobID), nil
}

// fakeQueue is a test double for JobEnqueuer.
type fakeQueue struct {
	enqueueFn func(ctx context.Context, job pkg.Job) error
	calls     int
	lastJob   pkg.Job
}

func (f *fakeQueue) EnqueueJob(ctx context.Context, job pkg.Job) error {
	f.calls++
	f.lastJob = job
	if f.enqueueFn != nil {
		return f.enqueueFn(ctx, job)
	}
	return nil
}

// fakeStore is a test double for JobStore, backed by an in-memory map.
type fakeStore struct {
	jobs map[string]pkg.Job

	recordErr error
	updateErr error
	deleteErr error

	recordCalls int
	updateCalls int
	deleteCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: make(map[string]pkg.Job)}
}

func (f *fakeStore) RecordJobMetadata(ctx context.Context, job pkg.Job) error {
	f.recordCalls++
	if f.recordErr != nil {
		return f.recordErr
	}
	f.jobs[job.ID] = job
	return nil
}

func (f *fakeStore) UpdateJobStatus(ctx context.Context, jobID string, status pkg.JobStatus) error {
	f.updateCalls++
	if f.updateErr != nil {
		return f.updateErr
	}
	job, ok := f.jobs[jobID]
	if !ok {
		return errors.New("job not found")
	}
	job.Status = status
	f.jobs[jobID] = job
	return nil
}

func (f *fakeStore) DeleteJob(ctx context.Context, jobID string) error {
	f.deleteCalls++
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.jobs, jobID)
	return nil
}

// testHandlerDeps bundles the fakes behind a freshly built UploadHandler so
// individual tests can assert on call counts/ordering.
type testHandlerDeps struct {
	handler  *UploadHandler
	uploader *fakeUploader
	queue    *fakeQueue
	store    *fakeStore
}

func newTestHandler() *testHandlerDeps {
	d := &testHandlerDeps{
		uploader: &fakeUploader{},
		queue:    &fakeQueue{},
		store:    newFakeStore(),
	}
	d.handler = NewUploadHandler(d.uploader, d.queue, d.store, "pulsegrid-output")
	return d
}

// multipartRequest builds a POST /videos/upload request. Pass videoBytes as
// nil to omit the video field entirely.
func multipartRequest(t *testing.T, fields map[string]string, videoBytes []byte) *http.Request {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	if videoBytes != nil {
		fw, err := w.CreateFormFile("video", "source.mp4")
		if err != nil {
			t.Fatalf("create form file: %v", err)
		}
		if _, err := fw.Write(videoBytes); err != nil {
			t.Fatalf("write video bytes: %v", err)
		}
	}

	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatalf("write field %s: %v", k, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

func TestUploadHandler_ValidRequest_Returns202(t *testing.T) {
	d := newTestHandler()
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp UploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.JobID == "" {
		t.Error("expected non-empty job_id")
	}
	if !strings.Contains(resp.StatusURI, resp.JobID) {
		t.Errorf("status_uri %q does not reference job_id %q", resp.StatusURI, resp.JobID)
	}
	if resp.SubmissionTime == "" {
		t.Error("expected non-empty submission_time")
	}

	if d.uploader.calls != 1 {
		t.Errorf("uploader called %d times, want 1", d.uploader.calls)
	}
	if d.queue.calls != 1 {
		t.Errorf("queue called %d times, want 1", d.queue.calls)
	}
	if d.store.recordCalls != 1 {
		t.Errorf("store.RecordJobMetadata called %d times, want 1", d.store.recordCalls)
	}
	if d.store.updateCalls != 1 {
		t.Errorf("store.UpdateJobStatus called %d times, want 1", d.store.updateCalls)
	}
	if got := d.store.jobs[resp.JobID].Status; got != pkg.JobStatusSubmitted {
		t.Errorf("final job status = %q, want %q", got, pkg.JobStatusSubmitted)
	}
}

func TestUploadHandler_MissingSourceName_Returns400(t *testing.T) {
	d := newTestHandler()
	req := multipartRequest(t, map[string]string{}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
	if d.uploader.calls != 0 {
		t.Errorf("uploader should not be called on validation failure, got %d calls", d.uploader.calls)
	}
}

func TestUploadHandler_FileExceedsLimit_Returns413(t *testing.T) {
	d := newTestHandler()
	d.handler.MaxUploadBytes = 10 // easy to exceed
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("this payload is definitely more than 10 bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
}

func TestUploadHandler_InvalidRenditionJSON_Returns400(t *testing.T) {
	d := newTestHandler()
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  "{not valid json",
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
}

func TestUploadHandler_MissingVideoFile_Returns400(t *testing.T) {
	d := newTestHandler()
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, nil)
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestUploadHandler_ValidRenditionsJSON_Accepted(t *testing.T) {
	d := newTestHandler()
	renditions := `[{"id":"1080p","codec":"libx264","bitrate_kbps":8000,"width":1920,"height":1080}]`
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  renditions,
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if len(d.queue.lastJob.Renditions) != 1 || d.queue.lastJob.Renditions[0].ID != "1080p" {
		t.Errorf("enqueued job renditions = %+v, want custom 1080p rendition", d.queue.lastJob.Renditions)
	}
}

func TestUploadHandler_InvalidRenditionSchema_Returns400(t *testing.T) {
	d := newTestHandler()
	// missing required codec/bitrate/width/height for a non-HLS rendition
	renditions := `[{"id":"1080p"}]`
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  renditions,
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestUploadHandler_WrongMethod_Returns405(t *testing.T) {
	d := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/videos/upload", nil)
	rec := httptest.NewRecorder()

	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// --- Task 6.1: DB-Kafka write order tests ---

// TestUploadHandler_S3UploadFails_NoDBOrKafkaWrites verifies that when the S3
// upload fails, neither the DB insert nor the Kafka publish happen at all
// (S3 upload precedes both in the write order).
func TestUploadHandler_S3UploadFails_NoDBOrKafkaWrites(t *testing.T) {
	d := newTestHandler()
	d.uploader.uploadFn = func(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error) {
		return "", errors.New("s3 unavailable")
	}

	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if d.store.recordCalls != 0 {
		t.Errorf("RecordJobMetadata called %d times, want 0 (S3 upload must fail before DB insert)", d.store.recordCalls)
	}
	if d.queue.calls != 0 {
		t.Errorf("EnqueueJob called %d times, want 0", d.queue.calls)
	}
}

// TestUploadHandler_DBInsertFails_NoKafkaPublish verifies that when the
// initial DB insert (status=submitting) fails, the job is never published to
// Kafka.
func TestUploadHandler_DBInsertFails_NoKafkaPublish(t *testing.T) {
	d := newTestHandler()
	d.store.recordErr = errors.New("db unavailable")

	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if d.queue.calls != 0 {
		t.Errorf("EnqueueJob called %d times, want 0 (DB insert must succeed before Kafka publish)", d.queue.calls)
	}
}

// TestUploadHandler_KafkaPublishFails_RollsBackDBInsert is Property/Task 6.1:
// mock Kafka publish failure and verify the DB transaction is rolled back —
// the job row is deleted so it never existed from the client's view.
func TestUploadHandler_KafkaPublishFails_RollsBackDBInsert(t *testing.T) {
	d := newTestHandler()
	d.queue.enqueueFn = func(ctx context.Context, job pkg.Job) error {
		return errors.New("kafka unavailable")
	}

	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if d.store.recordCalls != 1 {
		t.Errorf("RecordJobMetadata called %d times, want 1", d.store.recordCalls)
	}
	if d.store.deleteCalls != 1 {
		t.Errorf("DeleteJob called %d times, want 1 (rollback on Kafka publish failure)", d.store.deleteCalls)
	}
	if len(d.store.jobs) != 0 {
		t.Errorf("expected job row rolled back, but %d row(s) remain: %+v", len(d.store.jobs), d.store.jobs)
	}
	if d.store.updateCalls != 0 {
		t.Errorf("UpdateJobStatus called %d times, want 0 (never reached after Kafka publish failure)", d.store.updateCalls)
	}
}

// TestUploadHandler_DBUpdateFailsAfterKafka_JobStillSucceedsWithAlert is
// Task 6.1's second scenario: mock the final DB status update failing after
// a successful Kafka publish. The job already exists in the queue, so the
// client still gets a 202 (an operator alert is logged instead of failing
// an already-queued job), but the row is left at status="submitting" as the
// orphan flag for reconciliation.
func TestUploadHandler_DBUpdateFailsAfterKafka_JobStillSucceedsWithAlert(t *testing.T) {
	d := newTestHandler()
	d.store.updateErr = errors.New("db unavailable")

	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if d.queue.calls != 1 {
		t.Errorf("EnqueueJob called %d times, want 1", d.queue.calls)
	}
	if d.store.updateCalls != 1 {
		t.Errorf("UpdateJobStatus called %d times, want 1", d.store.updateCalls)
	}

	var resp UploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got := d.store.jobs[resp.JobID].Status; got != pkg.JobStatusSubmitting {
		t.Errorf("job status = %q, want %q (orphan flag: queued but DB update failed)", got, pkg.JobStatusSubmitting)
	}
}
