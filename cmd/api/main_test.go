package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// --- GET /jobs tests ---

// mockDBClient implements pkg.DBClient for testing handleListJobs.
type mockDBClient struct {
	queryResult pkg.JobListResult
	queryErr    error
	lastFilter  pkg.JobFilter
}

func (m *mockDBClient) RecordJobMetadata(ctx context.Context, job pkg.Job) error { return nil }
func (m *mockDBClient) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	return nil
}
func (m *mockDBClient) GetJobByID(ctx context.Context, jobID string) (pkg.Job, error) {
	return pkg.Job{}, nil
}
func (m *mockDBClient) InsertJobTx(ctx context.Context, job pkg.Job) (pkg.TxHandle, error) {
	return nil, nil
}
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

