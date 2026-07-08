package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"pulsegrid/pkg"
)

func TestUploadSuccess_DefaultRenditions(t *testing.T) {
	body, contentType := createMultipartForm(t, "test.mp4", []byte("fake video data"), "my_video", "")

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp UploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.JobID == "" {
		t.Fatal("job_id should not be empty")
	}
	if !strings.HasPrefix(resp.StatusURI, "/jobs/") {
		t.Fatalf("status_uri should start with /jobs/, got %s", resp.StatusURI)
	}
	if resp.EstimatedWaitTimeSeconds != 120 {
		t.Fatalf("expected estimated_wait_time_seconds=120, got %d", resp.EstimatedWaitTimeSeconds)
	}
	if resp.SubmissionTime == "" {
		t.Fatal("submission_time should not be empty")
	}
}

func TestUploadSuccess_CustomRenditions(t *testing.T) {
	renditions := `[{"id":"1080p","resolution":"1920x1080","video_codec":"libx264","video_bitrate":"8M"}]`
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "source1", renditions)

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUploadMissingVideo(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("source_name", "test")
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.ErrorCode != "VALIDATION_ERROR" {
		t.Fatalf("expected VALIDATION_ERROR, got %s", errResp.ErrorCode)
	}
}

func TestUploadMissingSourceName(t *testing.T) {
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "", "")

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if !strings.Contains(errResp.Error, "source_name") {
		t.Fatalf("error should mention source_name: %s", errResp.Error)
	}
}

func TestUploadInvalidRenditionsJSON(t *testing.T) {
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "src", "not json")

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.ErrorCode != "VALIDATION_ERROR" {
		t.Fatalf("expected VALIDATION_ERROR, got %s", errResp.ErrorCode)
	}
}

func TestUploadRenditionsMissingIDAndType(t *testing.T) {
	renditions := `[{"resolution":"720p"}]`
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "src", renditions)

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if !strings.Contains(errResp.Detail, "id") || !strings.Contains(errResp.Detail, "type") {
		t.Fatalf("detail should mention id/type requirement: %s", errResp.Detail)
	}
}

func TestUploadEmptyRenditionsArray(t *testing.T) {
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "src", "[]")

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestUploadWrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/videos/upload", nil)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestErrorResponseFormat(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/videos/upload", nil)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	var errResp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("should decode as ErrorResponse: %v", err)
	}
	if errResp.Error == "" {
		t.Fatal("error field should not be empty")
	}
	if errResp.ErrorCode == "" {
		t.Fatal("error_code field should not be empty")
	}
	if errResp.RequestID == "" {
		t.Fatal("request_id field should not be empty")
	}
	if errResp.Timestamp == "" {
		t.Fatal("timestamp field should not be empty")
	}
}

func TestParseRenditions_Empty(t *testing.T) {
	renditions, err := parseRenditions("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(renditions) != 3 {
		t.Fatalf("expected 3 default renditions, got %d", len(renditions))
	}
}

func TestParseRenditions_Valid(t *testing.T) {
	input := `[{"id":"720p","resolution":"1280x720"},{"type":"hls_segments"}]`
	renditions, err := parseRenditions(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(renditions) != 2 {
		t.Fatalf("expected 2 renditions, got %d", len(renditions))
	}
}

func TestParseRenditions_InvalidJSON(t *testing.T) {
	_, err := parseRenditions("{bad")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseRenditions_MissingIDAndType(t *testing.T) {
	_, err := parseRenditions(`[{"resolution":"720p"}]`)
	if err == nil {
		t.Fatal("expected error for rendition missing id and type")
	}
}

// createMultipartForm builds a multipart form body with video file and fields.
func createMultipartForm(t *testing.T, filename string, fileData []byte, sourceName, renditions string) (io.Reader, string) {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if sourceName != "" {
		writer.WriteField("source_name", sourceName)
	}
	if renditions != "" {
		writer.WriteField("renditions", renditions)
	}

	if filename != "" {
		part, err := writer.CreateFormFile("video", filename)
		if err != nil {
			t.Fatalf("create form file: %v", err)
		}
		if _, err := io.Copy(part, bytes.NewReader(fileData)); err != nil {
			t.Fatalf("write file data: %v", err)
		}
	}

	writer.Close()
	return body, writer.FormDataContentType()
}

func TestUploadFileExceeds10GB_413(t *testing.T) {
	// Override MaxUploadSize to a small value for fast test execution.
	// This validates the MaxBytesReader → 413 code path without streaming 10GB.
	originalMax := MaxUploadSize
	MaxUploadSize = 1024 // 1KB limit for test
	defer func() { MaxUploadSize = originalMax }()

	// Strategy: use io.Pipe so the multipart writer streams a file part
	// larger than the (overridden) limit. MaxBytesReader will cut off reading,
	// causing ParseMultipartForm to return "http: request body too large".
	pr, pw := io.Pipe()
	mpWriter := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		mpWriter.WriteField("source_name", "huge_video")
		part, err := mpWriter.CreateFormFile("video", "huge.mp4")
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		// Write data exceeding the test limit (1KB). 4KB is more than enough.
		io.CopyN(part, zeroReader{}, 4096)
		mpWriter.Close()
	}()

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", pr)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())
	req.ContentLength = 4096
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", rec.Code, rec.Body.String())
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.ErrorCode != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("expected PAYLOAD_TOO_LARGE error code, got %s", errResp.ErrorCode)
	}
}

// zeroReader is an io.Reader that returns an infinite stream of zero bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// Suppress unused import warnings for fmt.
var _ = fmt.Sprintf

// --- DB-Kafka atomic write order tests ---

// mockTxHandle tracks calls to UpdateStatusAndCommit and Rollback.
type mockTxHandle struct {
	commitErr    error
	committed    bool
	rolledBack   bool
}

func (h *mockTxHandle) UpdateStatusAndCommit(ctx context.Context, jobID string, status pkg.JobStatus) error {
	if h.commitErr != nil {
		return h.commitErr
	}
	h.committed = true
	return nil
}

func (h *mockTxHandle) Rollback(ctx context.Context) error {
	h.rolledBack = true
	return nil
}

// mockDBClientAtomic implements pkg.DBClient, returning a controlled TxHandle.
type mockDBClientAtomic struct {
	txHandle      *mockTxHandle
	insertTxErr   error
	statusEvents  []string
}

func (m *mockDBClientAtomic) RecordJobMetadata(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockDBClientAtomic) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	m.statusEvents = append(m.statusEvents, eventType)
	return nil
}
func (m *mockDBClientAtomic) GetJobByID(ctx context.Context, jobID string) (pkg.Job, error) {
	return pkg.Job{}, nil
}
func (m *mockDBClientAtomic) QueryJobs(ctx context.Context, filter pkg.JobFilter) (pkg.JobListResult, error) {
	return pkg.JobListResult{}, nil
}
func (m *mockDBClientAtomic) Ping(ctx context.Context) error { return nil }
func (m *mockDBClientAtomic) Close() {}
func (m *mockDBClientAtomic) InsertJobTx(ctx context.Context, job pkg.Job) (pkg.TxHandle, error) {
	if m.insertTxErr != nil {
		return nil, m.insertTxErr
	}
	return m.txHandle, nil
}

// mockKafkaFailing always returns error on EnqueueJob.
type mockKafkaFailing struct {
	enqueueErr error
}

func (m *mockKafkaFailing) EnqueueJob(ctx context.Context, job pkg.Job) error {
	return m.enqueueErr
}
func (m *mockKafkaFailing) SendDLQ(ctx context.Context, job pkg.Job, reason string) error {
	return nil
}
func (m *mockKafkaFailing) Ping(ctx context.Context) error { return m.enqueueErr }
func (m *mockKafkaFailing) Close() error { return nil }

// mockKafkaSuccess always succeeds on EnqueueJob.
type mockKafkaSuccess struct{}

func (m *mockKafkaSuccess) EnqueueJob(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockKafkaSuccess) SendDLQ(ctx context.Context, job pkg.Job, reason string) error {
	return nil
}
func (m *mockKafkaSuccess) Ping(ctx context.Context) error { return nil }
func (m *mockKafkaSuccess) Close() error { return nil }

func TestUpload_KafkaFail_DBRolledBack(t *testing.T) {
	// Setup: mock DB returns TxHandle, mock Kafka fails.
	txH := &mockTxHandle{}
	mockDB := &mockDBClientAtomic{txHandle: txH}
	mockKafka := &mockKafkaFailing{enqueueErr: fmt.Errorf("kafka broker unavailable")}

	oldDB := dbClient
	oldKafka := kafkaProducer
	dbClient = mockDB
	kafkaProducer = mockKafka
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
	}()

	body, contentType := createMultipartForm(t, "test.mp4", []byte("data"), "source_vid", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	// Expect 500 error because Kafka failed.
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify DB transaction was rolled back (job never persisted).
	if !txH.rolledBack {
		t.Fatal("expected DB transaction to be rolled back after Kafka failure")
	}
	if txH.committed {
		t.Fatal("DB transaction should NOT be committed when Kafka fails")
	}
}

func TestUpload_DBCommitFail_AlertLogged(t *testing.T) {
	// Setup: mock DB returns TxHandle that fails on commit, Kafka succeeds.
	txH := &mockTxHandle{commitErr: fmt.Errorf("connection reset during commit")}
	mockDB := &mockDBClientAtomic{txHandle: txH}
	mockKafka := &mockKafkaSuccess{}

	oldDB := dbClient
	oldKafka := kafkaProducer
	dbClient = mockDB
	kafkaProducer = mockKafka
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
	}()

	// Capture log output to verify ALERT is logged.
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	body, contentType := createMultipartForm(t, "test.mp4", []byte("data"), "source_vid", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	// Should still return 202 (job IS in Kafka queue).
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 even after DB commit failure, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify ALERT was logged about orphan in queue.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "ALERT") {
		t.Fatalf("expected ALERT in log output, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "orphan") {
		t.Fatalf("expected 'orphan' mention in log output, got: %s", logOutput)
	}
}

// --- GET /jobs tests ---

// mockDBClient implements pkg.DBClient for testing handleListJobs and handleGetJob.
type mockDBClient struct {
	queryResult  pkg.JobListResult
	queryErr     error
	lastFilter   pkg.JobFilter
	getJobResult pkg.Job
	getJobErr    error
}

func (m *mockDBClient) RecordJobMetadata(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockDBClient) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	return nil
}
func (m *mockDBClient) GetJobByID(ctx context.Context, jobID string) (pkg.Job, error) {
	return m.getJobResult, m.getJobErr
}
func (m *mockDBClient) InsertJobTx(ctx context.Context, job pkg.Job) (pkg.TxHandle, error) {
	return nil, nil
}
func (m *mockDBClient) Ping(ctx context.Context) error { return nil }
func (m *mockDBClient) Close() {}
func (m *mockDBClient) QueryJobs(ctx context.Context, filter pkg.JobFilter) (pkg.JobListResult, error) {
	m.lastFilter = filter
	return m.queryResult, m.queryErr
}

func TestListJobs_Success(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completionTime := now.Add(5 * time.Minute)
	dur := completionTime.Sub(now).Seconds()

	mock := &mockDBClient{
		queryResult: pkg.JobListResult{
			Jobs: []pkg.JobSummary{
				{JobID: "job-1", Status: "completed", SubmissionTime: now, CompletionTime: &completionTime, DurationSeconds: &dur},
				{JobID: "job-2", Status: "processing", SubmissionTime: now},
			},
			Total:  2,
			Limit:  100,
			Offset: 0,
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=completed,processing&limit=50&offset=0", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp ListJobsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("expected total=2, got %d", resp.Total)
	}
	if resp.Limit != 100 {
		t.Fatalf("expected limit=100, got %d", resp.Limit)
	}
	if len(resp.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(resp.Jobs))
	}

	// Verify filter was passed correctly.
	if len(mock.lastFilter.Statuses) != 2 {
		t.Fatalf("expected 2 status filters, got %d", len(mock.lastFilter.Statuses))
	}
	if mock.lastFilter.Limit != 50 {
		t.Fatalf("expected limit=50 in filter, got %d", mock.lastFilter.Limit)
	}
}

func TestListJobs_InvalidSubmittedAfter(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=not-a-date", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.ErrorCode != "VALIDATION_ERROR" {
		t.Fatalf("expected VALIDATION_ERROR, got %s", errResp.ErrorCode)
	}
}

func TestListJobs_InvalidSubmittedBefore(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_before=invalid", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_AfterExceedsBefore(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=2024-12-01T00:00:00Z&submitted_before=2024-01-01T00:00:00Z", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListJobs_InvalidStatus(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=invalid_status", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_LimitExceedsMax(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=5000", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_InvalidLimit(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=-1", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_InvalidOffset(t *testing.T) {
	oldDB := dbClient
	dbClient = &mockDBClient{}
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?offset=-5", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_NoDB(t *testing.T) {
	oldDB := dbClient
	dbClient = nil
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestListJobs_WrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListJobs_DefaultLimitOffset(t *testing.T) {
	mock := &mockDBClient{
		queryResult: pkg.JobListResult{
			Jobs:   []pkg.JobSummary{},
			Total:  0,
			Limit:  100,
			Offset: 0,
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if mock.lastFilter.Limit != 100 {
		t.Fatalf("expected default limit=100, got %d", mock.lastFilter.Limit)
	}
	if mock.lastFilter.Offset != 0 {
		t.Fatalf("expected default offset=0, got %d", mock.lastFilter.Offset)
	}
}

func TestListJobs_TimestampFilters(t *testing.T) {
	mock := &mockDBClient{
		queryResult: pkg.JobListResult{
			Jobs:   []pkg.JobSummary{},
			Total:  0,
			Limit:  100,
			Offset: 0,
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=2024-01-01T00:00:00Z&submitted_before=2024-12-31T23:59:59Z", nil)
	rec := httptest.NewRecorder()

	handleListJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if mock.lastFilter.SubmittedAfter == nil {
		t.Fatal("expected submitted_after to be set")
	}
	if mock.lastFilter.SubmittedBefore == nil {
		t.Fatal("expected submitted_before to be set")
	}
}


// --- GET /jobs/{job_id} tests ---

func TestGetJob_Completed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completionTime := now.Add(5 * time.Minute)

	mock := &mockDBClient{
		getJobResult: pkg.Job{
			JobID:          "550e8400-e29b-41d4-a716-446655440000",
			Status:         pkg.JobStatusCompleted,
			SubmissionTime: now,
			CompletionTime: &completionTime,
			RetryCount:     0,
			OutputFiles: []pkg.OutputFile{
				{Rendition: "720p", Path: "s3://pulsegrid-output/550e8400/720p/720p.mp4", SizeBytes: 524288000, DurationSeconds: 300},
				{Rendition: "480p", Path: "s3://pulsegrid-output/550e8400/480p/480p.mp4", SizeBytes: 262144000, DurationSeconds: 300},
			},
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/550e8400-e29b-41d4-a716-446655440000", nil)
	rec := httptest.NewRecorder()

	handleGetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp JobStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.JobID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("expected job_id=550e8400-e29b-41d4-a716-446655440000, got %s", resp.JobID)
	}
	if resp.Status != pkg.JobStatusCompleted {
		t.Fatalf("expected status=completed, got %s", resp.Status)
	}
	if resp.CompletionTime == nil {
		t.Fatal("expected completion_time to be set for completed job")
	}
	if len(resp.OutputFiles) != 2 {
		t.Fatalf("expected 2 output_files, got %d", len(resp.OutputFiles))
	}
	if resp.OutputFiles[0].Rendition != "720p" {
		t.Fatalf("expected first output rendition=720p, got %s", resp.OutputFiles[0].Rendition)
	}
	if resp.FailureReason != nil {
		t.Fatalf("expected no failure_reason for completed job, got %s", *resp.FailureReason)
	}
}

func TestGetJob_Processing(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	mock := &mockDBClient{
		getJobResult: pkg.Job{
			JobID:          "661e8400-e29b-41d4-a716-446655440001",
			Status:         pkg.JobStatusProcessing,
			SubmissionTime: now,
			RetryCount:     0,
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/661e8400-e29b-41d4-a716-446655440001", nil)
	rec := httptest.NewRecorder()

	handleGetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp JobStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Status != pkg.JobStatusProcessing {
		t.Fatalf("expected status=processing, got %s", resp.Status)
	}
	if resp.CompletionTime != nil {
		t.Fatalf("expected no completion_time for processing job, got %s", *resp.CompletionTime)
	}
	if len(resp.OutputFiles) != 0 {
		t.Fatalf("expected no output_files for processing job, got %d", len(resp.OutputFiles))
	}
}

func TestGetJob_NotFound(t *testing.T) {
	mock := &mockDBClient{
		getJobErr: pkg.ErrJobNotFound,
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/nonexistent-id", nil)
	rec := httptest.NewRecorder()

	handleGetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.ErrorCode != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND error code, got %s", errResp.ErrorCode)
	}
}

func TestGetJob_Failed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completionTime := now.Add(2 * time.Minute)

	mock := &mockDBClient{
		getJobResult: pkg.Job{
			JobID:          "772e8400-e29b-41d4-a716-446655440002",
			Status:         pkg.JobStatusFailed,
			SubmissionTime: now,
			CompletionTime: &completionTime,
			RetryCount:     3,
			FailureReason:  "ffmpeg: unsupported codec - VP9",
		},
	}

	oldDB := dbClient
	dbClient = mock
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/772e8400-e29b-41d4-a716-446655440002", nil)
	rec := httptest.NewRecorder()

	handleGetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp JobStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Status != pkg.JobStatusFailed {
		t.Fatalf("expected status=failed, got %s", resp.Status)
	}
	if resp.FailureReason == nil {
		t.Fatal("expected failure_reason to be set for failed job")
	}
	if *resp.FailureReason != "ffmpeg: unsupported codec - VP9" {
		t.Fatalf("expected failure_reason='ffmpeg: unsupported codec - VP9', got %s", *resp.FailureReason)
	}
	if resp.RetryCount != 3 {
		t.Fatalf("expected retry_count=3, got %d", resp.RetryCount)
	}
}


// --- Prometheus Metrics Emission Tests (Task 9.1) ---

func TestUpload_MetricsEmitted_CounterIncrements(t *testing.T) {
	// Use custom registry for test isolation.
	reg := prometheus.NewRegistry()
	testMetrics := pkg.NewMetrics(reg)

	oldMetrics := apiMetrics
	apiMetrics = testMetrics
	defer func() { apiMetrics = oldMetrics }()

	// First upload.
	body, contentType := createMultipartForm(t, "video1.mp4", []byte("data1"), "src1", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify counter = 1.
	counterVal := getCounterValue(t, reg, "pulsegrid_jobs_submitted_total")
	if counterVal != 1 {
		t.Fatalf("expected counter=1 after first upload, got %f", counterVal)
	}

	// Second upload.
	body2, contentType2 := createMultipartForm(t, "video2.mp4", []byte("data2"), "src2", "")
	req2 := httptest.NewRequest(http.MethodPost, "/videos/upload", body2)
	req2.Header.Set("Content-Type", contentType2)
	rec2 := httptest.NewRecorder()
	handleVideoUpload(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec2.Code, rec2.Body.String())
	}

	// Verify counter = 2.
	counterVal = getCounterValue(t, reg, "pulsegrid_jobs_submitted_total")
	if counterVal != 2 {
		t.Fatalf("expected counter=2 after second upload, got %f", counterVal)
	}
}

func TestUpload_MetricsEmitted_HistogramRecordsDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	testMetrics := pkg.NewMetrics(reg)

	oldMetrics := apiMetrics
	apiMetrics = testMetrics
	defer func() { apiMetrics = oldMetrics }()

	body, contentType := createMultipartForm(t, "vid.mp4", []byte("video content"), "myfile", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify histogram has 1 observation.
	histCount, histSum := getHistogramValues(t, reg, "pulsegrid_upload_duration_seconds")
	if histCount != 1 {
		t.Fatalf("expected histogram count=1, got %d", histCount)
	}
	if histSum < 0 {
		t.Fatalf("expected histogram sum >= 0, got %f", histSum)
	}
}

func TestUpload_MetricsNotEmitted_OnFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	testMetrics := pkg.NewMetrics(reg)

	oldMetrics := apiMetrics
	apiMetrics = testMetrics
	defer func() { apiMetrics = oldMetrics }()

	// Send invalid request (missing source_name) — should 400, no metrics.
	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	handleVideoUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// Counter should still be 0 — no increment on failed upload.
	counterVal := getCounterValue(t, reg, "pulsegrid_jobs_submitted_total")
	if counterVal != 0 {
		t.Fatalf("expected counter=0 after failed upload, got %f", counterVal)
	}

	// Histogram should have 0 observations.
	histCount, _ := getHistogramValues(t, reg, "pulsegrid_upload_duration_seconds")
	if histCount != 0 {
		t.Fatalf("expected histogram count=0 after failed upload, got %d", histCount)
	}
}

func TestUpload_HistogramBuckets_Correct(t *testing.T) {
	reg := prometheus.NewRegistry()
	testMetrics := pkg.NewMetrics(reg)

	oldMetrics := apiMetrics
	apiMetrics = testMetrics
	defer func() { apiMetrics = oldMetrics }()

	body, contentType := createMultipartForm(t, "vid.mp4", []byte("data"), "src", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify histogram bucket boundaries match DefaultHistogramBuckets.
	buckets := getHistogramBuckets(t, reg, "pulsegrid_upload_duration_seconds")
	expectedBuckets := pkg.DefaultHistogramBuckets
	if len(buckets) != len(expectedBuckets) {
		t.Fatalf("expected %d buckets, got %d", len(expectedBuckets), len(buckets))
	}
	for i, b := range expectedBuckets {
		if buckets[i] != b {
			t.Fatalf("bucket[%d]: expected %f, got %f", i, b, buckets[i])
		}
	}
}

// --- Helper functions for reading Prometheus metrics from a custom registry ---

func getCounterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range metrics {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func getHistogramValues(t *testing.T, reg *prometheus.Registry, name string) (uint64, float64) {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range metrics {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if m.GetHistogram() != nil {
					return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	return 0, 0
}

func getHistogramBuckets(t *testing.T, reg *prometheus.Registry, name string) []float64 {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range metrics {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if m.GetHistogram() != nil {
					var bounds []float64
					for _, b := range m.GetHistogram().GetBucket() {
						bounds = append(bounds, b.GetUpperBound())
					}
					return bounds
				}
			}
		}
	}
	return nil
}


// --- GET /health tests ---

// mockS3UploaderHealth implements pkg.S3Uploader for health check tests.
type mockS3UploaderHealth struct {
	pingErr error
}

func (m *mockS3UploaderHealth) UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error) {
	return "", nil
}
func (m *mockS3UploaderHealth) Ping(ctx context.Context) error { return m.pingErr }

// mockDBClientHealth implements pkg.DBClient for health check tests.
type mockDBClientHealth struct {
	pingErr error
}

func (m *mockDBClientHealth) RecordJobMetadata(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockDBClientHealth) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	return nil
}
func (m *mockDBClientHealth) GetJobByID(ctx context.Context, jobID string) (pkg.Job, error) {
	return pkg.Job{}, nil
}
func (m *mockDBClientHealth) QueryJobs(ctx context.Context, filter pkg.JobFilter) (pkg.JobListResult, error) {
	return pkg.JobListResult{}, nil
}
func (m *mockDBClientHealth) InsertJobTx(ctx context.Context, job pkg.Job) (pkg.TxHandle, error) {
	return nil, nil
}
func (m *mockDBClientHealth) Ping(ctx context.Context) error { return m.pingErr }
func (m *mockDBClientHealth) Close()                         {}

// mockKafkaHealth implements pkg.KafkaProducer for health check tests.
type mockKafkaHealth struct {
	pingErr error
}

func (m *mockKafkaHealth) EnqueueJob(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockKafkaHealth) SendDLQ(ctx context.Context, job pkg.Job, reason string) error {
	return nil
}
func (m *mockKafkaHealth) Ping(ctx context.Context) error { return m.pingErr }
func (m *mockKafkaHealth) Close() error                   { return nil }

func TestHealth_AllHealthy(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = &mockDBClientHealth{}
	kafkaProducer = &mockKafkaHealth{}
	s3Uploader = &mockS3UploaderHealth{}
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "healthy" {
		t.Fatalf("expected status=healthy, got %s", resp.Status)
	}
	if resp.Checks["postgres"].Status != "healthy" {
		t.Fatalf("expected postgres=healthy, got %s", resp.Checks["postgres"].Status)
	}
	if resp.Checks["kafka"].Status != "healthy" {
		t.Fatalf("expected kafka=healthy, got %s", resp.Checks["kafka"].Status)
	}
	if resp.Checks["s3"].Status != "healthy" {
		t.Fatalf("expected s3=healthy, got %s", resp.Checks["s3"].Status)
	}
}

func TestHealth_PostgresDown(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = &mockDBClientHealth{pingErr: fmt.Errorf("connection refused")}
	kafkaProducer = &mockKafkaHealth{}
	s3Uploader = &mockS3UploaderHealth{}
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp HealthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "unhealthy" {
		t.Fatalf("expected status=unhealthy, got %s", resp.Status)
	}
	if resp.Checks["postgres"].Status != "unhealthy" {
		t.Fatalf("expected postgres=unhealthy, got %s", resp.Checks["postgres"].Status)
	}
	if resp.Checks["postgres"].Error == "" {
		t.Fatal("expected error message for postgres")
	}
}

func TestHealth_KafkaDown(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = &mockDBClientHealth{}
	kafkaProducer = &mockKafkaHealth{pingErr: fmt.Errorf("broker unreachable")}
	s3Uploader = &mockS3UploaderHealth{}
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp HealthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Checks["kafka"].Status != "unhealthy" {
		t.Fatalf("expected kafka=unhealthy, got %s", resp.Checks["kafka"].Status)
	}
}

func TestHealth_S3Down(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = &mockDBClientHealth{}
	kafkaProducer = &mockKafkaHealth{}
	s3Uploader = &mockS3UploaderHealth{pingErr: fmt.Errorf("bucket not found")}
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp HealthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Checks["s3"].Status != "unhealthy" {
		t.Fatalf("expected s3=unhealthy, got %s", resp.Checks["s3"].Status)
	}
}

func TestHealth_NotConfigured(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = nil
	kafkaProducer = nil
	s3Uploader = nil
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	// When nothing configured, all checks show "not_configured" — this is healthy (local dev).
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for not-configured mode, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp HealthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "healthy" {
		t.Fatalf("expected healthy when not configured, got %s", resp.Status)
	}
	if resp.Checks["postgres"].Status != "not_configured" {
		t.Fatalf("expected postgres=not_configured, got %s", resp.Checks["postgres"].Status)
	}
}

func TestHealth_MultipleDown(t *testing.T) {
	oldDB := dbClient
	oldKafka := kafkaProducer
	oldS3 := s3Uploader
	dbClient = &mockDBClientHealth{pingErr: fmt.Errorf("db down")}
	kafkaProducer = &mockKafkaHealth{pingErr: fmt.Errorf("kafka down")}
	s3Uploader = &mockS3UploaderHealth{pingErr: fmt.Errorf("s3 down")}
	defer func() {
		dbClient = oldDB
		kafkaProducer = oldKafka
		s3Uploader = oldS3
	}()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp HealthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Checks["postgres"].Status != "unhealthy" {
		t.Fatalf("expected postgres=unhealthy, got %s", resp.Checks["postgres"].Status)
	}
	if resp.Checks["kafka"].Status != "unhealthy" {
		t.Fatalf("expected kafka=unhealthy, got %s", resp.Checks["kafka"].Status)
	}
	if resp.Checks["s3"].Status != "unhealthy" {
		t.Fatalf("expected s3=unhealthy, got %s", resp.Checks["s3"].Status)
	}
}

func TestHealth_WrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()
	handleHealth(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// =============================================================================
// Integration Tests: Full Upload Flow & Status Query (Checkpoint 11)
// All three dependencies (S3, Kafka, DB) mocked and wired together.
// =============================================================================

// mockS3Full implements S3Uploader, tracking calls for verification.
type mockS3Full struct {
	uploadCalled bool
	lastJobID    string
	lastSource   string
	uploadErr    error
}

func (m *mockS3Full) UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error) {
	if m.uploadErr != nil {
		return "", m.uploadErr
	}
	m.uploadCalled = true
	m.lastJobID = jobID
	m.lastSource = sourceName
	return fmt.Sprintf("s3://pulsegrid-source/%s/original.mp4", jobID), nil
}

func (m *mockS3Full) Ping(ctx context.Context) error { return nil }

// mockKafkaFull implements KafkaProducer, tracking enqueued jobs.
type mockKafkaFull struct {
	enqueuedJobs []pkg.Job
	enqueueErr   error
}

func (m *mockKafkaFull) EnqueueJob(ctx context.Context, job pkg.Job) error {
	if m.enqueueErr != nil {
		return m.enqueueErr
	}
	m.enqueuedJobs = append(m.enqueuedJobs, job)
	return nil
}

func (m *mockKafkaFull) SendDLQ(ctx context.Context, job pkg.Job, reason string) error { return nil }
func (m *mockKafkaFull) Ping(ctx context.Context) error                                { return nil }
func (m *mockKafkaFull) Close() error                                                  { return nil }

// mockDBFull implements DBClient with in-memory storage for integration tests.
type mockDBFull struct {
	jobs         map[string]pkg.Job
	statusEvents []string
	insertTxErr  error
}

func newMockDBFull() *mockDBFull {
	return &mockDBFull{jobs: make(map[string]pkg.Job)}
}

func (m *mockDBFull) RecordJobMetadata(ctx context.Context, job pkg.Job) error {
	m.jobs[job.JobID] = job
	return nil
}

func (m *mockDBFull) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	m.statusEvents = append(m.statusEvents, eventType)
	return nil
}

func (m *mockDBFull) GetJobByID(ctx context.Context, jobID string) (pkg.Job, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return pkg.Job{}, pkg.ErrJobNotFound
	}
	return job, nil
}

func (m *mockDBFull) QueryJobs(ctx context.Context, filter pkg.JobFilter) (pkg.JobListResult, error) {
	return pkg.JobListResult{}, nil
}

func (m *mockDBFull) Ping(ctx context.Context) error { return nil }
func (m *mockDBFull) Close()                         {}

func (m *mockDBFull) InsertJobTx(ctx context.Context, job pkg.Job) (pkg.TxHandle, error) {
	if m.insertTxErr != nil {
		return nil, m.insertTxErr
	}
	// Store the job immediately (simulates INSERT within tx).
	m.jobs[job.JobID] = job
	return &mockDBFullTxHandle{db: m, jobID: job.JobID}, nil
}

// mockDBFullTxHandle simulates a DB transaction handle with in-memory store.
type mockDBFullTxHandle struct {
	db    *mockDBFull
	jobID string
}

func (h *mockDBFullTxHandle) UpdateStatusAndCommit(ctx context.Context, jobID string, status pkg.JobStatus) error {
	job := h.db.jobs[jobID]
	job.Status = status
	h.db.jobs[jobID] = job
	return nil
}

func (h *mockDBFullTxHandle) Rollback(ctx context.Context) error {
	delete(h.db.jobs, h.jobID)
	return nil
}

// TestIntegration_FullUploadFlow verifies complete upload pipeline:
// multipart parse → S3 upload → Kafka enqueue → DB insert → 202 with job_id.
func TestIntegration_FullUploadFlow(t *testing.T) {
	mockS3 := &mockS3Full{}
	mockKafka := &mockKafkaFull{}
	mockDB := newMockDBFull()

	oldS3 := s3Uploader
	oldKafka := kafkaProducer
	oldDB := dbClient
	s3Uploader = mockS3
	kafkaProducer = mockKafka
	dbClient = mockDB
	defer func() {
		s3Uploader = oldS3
		kafkaProducer = oldKafka
		dbClient = oldDB
	}()

	// Build multipart request with video + source_name + custom renditions.
	renditionsJSON := `[{"id":"720p","resolution":"1280x720","video_codec":"libx264","video_bitrate":"5M"}]`
	body, contentType := createMultipartForm(t, "interview_clip.mp4", []byte("fake video bytes"), "interview_2024", renditionsJSON)

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	handleVideoUpload(rec, req)

	// 1. Verify HTTP 202 response.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp UploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.JobID == "" {
		t.Fatal("job_id must not be empty")
	}
	if resp.StatusURI != "/jobs/"+resp.JobID {
		t.Fatalf("status_uri mismatch: expected /jobs/%s, got %s", resp.JobID, resp.StatusURI)
	}
	if resp.SubmissionTime == "" {
		t.Fatal("submission_time must not be empty")
	}

	// 2. Verify S3 upload called with correct job ID and source name.
	if !mockS3.uploadCalled {
		t.Fatal("S3 upload was not called")
	}
	if mockS3.lastJobID != resp.JobID {
		t.Fatalf("S3 received wrong jobID: expected %s, got %s", resp.JobID, mockS3.lastJobID)
	}
	if mockS3.lastSource != "interview_2024" {
		t.Fatalf("S3 received wrong source_name: expected interview_2024, got %s", mockS3.lastSource)
	}

	// 3. Verify Kafka enqueue called with correct job.
	if len(mockKafka.enqueuedJobs) != 1 {
		t.Fatalf("expected 1 Kafka enqueue, got %d", len(mockKafka.enqueuedJobs))
	}
	enqueuedJob := mockKafka.enqueuedJobs[0]
	if enqueuedJob.JobID != resp.JobID {
		t.Fatalf("Kafka job_id mismatch: expected %s, got %s", resp.JobID, enqueuedJob.JobID)
	}
	if enqueuedJob.SourceFileName != "interview_2024" {
		t.Fatalf("Kafka source_file_name mismatch: expected interview_2024, got %s", enqueuedJob.SourceFileName)
	}
	if len(enqueuedJob.Renditions) != 1 || enqueuedJob.Renditions[0].ID != "720p" {
		t.Fatalf("Kafka renditions mismatch: got %+v", enqueuedJob.Renditions)
	}
	if enqueuedJob.OutputS3Prefix == "" {
		t.Fatal("Kafka job output_s3_prefix must not be empty")
	}

	// 4. Verify DB job persisted with status='submitted' (after commit).
	storedJob, exists := mockDB.jobs[resp.JobID]
	if !exists {
		t.Fatal("job not found in DB after upload")
	}
	if storedJob.Status != pkg.JobStatusSubmitted {
		t.Fatalf("expected DB job status=submitted, got %s", storedJob.Status)
	}
	if storedJob.SourceFileName != "interview_2024" {
		t.Fatalf("DB source_file_name mismatch: expected interview_2024, got %s", storedJob.SourceFileName)
	}

	// 5. Verify status event recorded.
	found := false
	for _, evt := range mockDB.statusEvents {
		if evt == "submitted" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'submitted' status event, got events: %v", mockDB.statusEvents)
	}
}

// TestIntegration_StatusQuery verifies: create job in DB → GET /jobs/{id} → correct JSON.
func TestIntegration_StatusQuery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completionTime := now.Add(3 * time.Minute)

	mockDB := newMockDBFull()
	// Pre-populate DB with a completed job.
	mockDB.jobs["test-job-123"] = pkg.Job{
		JobID:               "test-job-123",
		Status:              pkg.JobStatusCompleted,
		SourceFileName:      "demo.mp4",
		SourceFileSizeBytes: 500000000,
		SourceS3URI:         "s3://pulsegrid-source/test-job-123/original.mp4",
		OutputS3Prefix:      "s3://pulsegrid-output/test-job-123/",
		SubmissionTime:      now,
		CompletionTime:      &completionTime,
		RetryCount:          0,
		OutputFiles: []pkg.OutputFile{
			{Rendition: "720p", Path: "s3://pulsegrid-output/test-job-123/720p/720p.mp4", SizeBytes: 250000000, DurationSeconds: 180},
			{Rendition: "480p", Path: "s3://pulsegrid-output/test-job-123/480p/480p.mp4", SizeBytes: 125000000, DurationSeconds: 180},
		},
	}

	oldDB := dbClient
	dbClient = mockDB
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/test-job-123", nil)
	rec := httptest.NewRecorder()

	handleGetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp JobStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.JobID != "test-job-123" {
		t.Fatalf("expected job_id=test-job-123, got %s", resp.JobID)
	}
	if resp.Status != pkg.JobStatusCompleted {
		t.Fatalf("expected status=completed, got %s", resp.Status)
	}
	if resp.CompletionTime == nil {
		t.Fatal("expected completion_time set")
	}
	if len(resp.OutputFiles) != 2 {
		t.Fatalf("expected 2 output files, got %d", len(resp.OutputFiles))
	}
	if resp.OutputFiles[0].Rendition != "720p" {
		t.Fatalf("expected first rendition=720p, got %s", resp.OutputFiles[0].Rendition)
	}
	if resp.FailureReason != nil {
		t.Fatalf("unexpected failure_reason: %s", *resp.FailureReason)
	}
}

// TestIntegration_UploadThenQuery verifies round-trip: upload → query same job.
func TestIntegration_UploadThenQuery(t *testing.T) {
	mockS3 := &mockS3Full{}
	mockKafka := &mockKafkaFull{}
	mockDB := newMockDBFull()

	oldS3 := s3Uploader
	oldKafka := kafkaProducer
	oldDB := dbClient
	s3Uploader = mockS3
	kafkaProducer = mockKafka
	dbClient = mockDB
	defer func() {
		s3Uploader = oldS3
		kafkaProducer = oldKafka
		dbClient = oldDB
	}()

	// Upload a job.
	body, contentType := createMultipartForm(t, "roundtrip.mp4", []byte("data"), "round_trip_test", "")
	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	handleVideoUpload(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("upload expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var uploadResp UploadResponse
	json.NewDecoder(rec.Body).Decode(&uploadResp)

	// Now query same job via GET /jobs/{id}.
	req2 := httptest.NewRequest(http.MethodGet, "/jobs/"+uploadResp.JobID, nil)
	rec2 := httptest.NewRecorder()
	handleGetJob(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("status query expected 200, got %d: %s", rec2.Code, rec2.Body.String())
	}

	var statusResp JobStatusResponse
	json.NewDecoder(rec2.Body).Decode(&statusResp)

	if statusResp.JobID != uploadResp.JobID {
		t.Fatalf("job_id mismatch: upload=%s, query=%s", uploadResp.JobID, statusResp.JobID)
	}
	if statusResp.Status != pkg.JobStatusSubmitted {
		t.Fatalf("expected status=submitted after upload, got %s", statusResp.Status)
	}
}

// TestIntegration_StatusQuery_NotFound verifies 404 for nonexistent job.
func TestIntegration_StatusQuery_NotFound(t *testing.T) {
	mockDB := newMockDBFull()

	oldDB := dbClient
	dbClient = mockDB
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist-999", nil)
	rec := httptest.NewRecorder()
	handleGetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}

	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.ErrorCode != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND, got %s", errResp.ErrorCode)
	}
}
