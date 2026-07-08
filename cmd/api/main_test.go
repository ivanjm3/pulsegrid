package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestQueryJobs_NoDBReturnsEmpty(t *testing.T) {
	// dbClient is nil by default in tests (local dev mode).
	oldDB := dbClient
	dbClient = nil
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp QueryJobsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Jobs) != 0 {
		t.Fatalf("expected empty jobs, got %d", len(resp.Jobs))
	}
	if resp.Limit != 100 {
		t.Fatalf("expected default limit 100, got %d", resp.Limit)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}
}

func TestQueryJobs_InvalidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_InvalidSubmittedAfter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_after=not-a-date", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.ErrorCode != "VALIDATION_ERROR" {
		t.Fatalf("expected VALIDATION_ERROR, got %s", errResp.ErrorCode)
	}
	if !strings.Contains(errResp.Error, "submitted_after") {
		t.Fatalf("error should mention submitted_after: %s", errResp.Error)
	}
}

func TestQueryJobs_InvalidSubmittedBefore(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?submitted_before=garbage", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_RangeInverted(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/jobs?submitted_after=2024-06-01T00:00:00Z&submitted_before=2024-01-01T00:00:00Z", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if !strings.Contains(errResp.Error, "before") {
		t.Fatalf("error should mention range issue: %s", errResp.Error)
	}
}

func TestQueryJobs_InvalidStatus(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?status=bogus", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if !strings.Contains(errResp.Error, "bogus") {
		t.Fatalf("error should mention invalid status: %s", errResp.Error)
	}
}

func TestQueryJobs_ValidStatuses(t *testing.T) {
	oldDB := dbClient
	dbClient = nil
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=submitted,completed", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryJobs_LimitExceedsMax(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=5000", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	var errResp ErrorResponse
	json.NewDecoder(rec.Body).Decode(&errResp)
	if !strings.Contains(errResp.Error, "1000") {
		t.Fatalf("error should mention max 1000: %s", errResp.Error)
	}
}

func TestQueryJobs_LimitZero(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=0", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_LimitNegative(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=-1", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_OffsetNegative(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?offset=-5", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_CustomLimitOffset(t *testing.T) {
	oldDB := dbClient
	dbClient = nil
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=50&offset=200", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp QueryJobsResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Limit != 50 {
		t.Fatalf("expected limit 50, got %d", resp.Limit)
	}
	if resp.Offset != 200 {
		t.Fatalf("expected offset 200, got %d", resp.Offset)
	}
}

func TestQueryJobs_ValidTimestamps(t *testing.T) {
	oldDB := dbClient
	dbClient = nil
	defer func() { dbClient = oldDB }()

	req := httptest.NewRequest(http.MethodGet,
		"/jobs?submitted_after=2024-01-01T00:00:00Z&submitted_before=2024-12-31T23:59:59Z", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestQueryJobs_LimitNotANumber(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=abc", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryJobs_OffsetNotANumber(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?offset=xyz", nil)
	rec := httptest.NewRecorder()

	handleQueryJobs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
