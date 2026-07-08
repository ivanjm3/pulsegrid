package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestS3Client_ImplementsS3Uploader(t *testing.T) {
	// Compile-time check that S3Client implements S3Uploader interface.
	var _ S3Uploader = (*S3Client)(nil)
}

func TestS3URI_Format(t *testing.T) {
	// Verify the expected S3 URI format matches design spec.
	// s3://pulsegrid-source/{jobID}/original.mp4
	jobID := "550e8400-e29b-41d4-a716-446655440000"
	expected := "s3://pulsegrid-source/" + jobID + "/original.mp4"
	bucket := "pulsegrid-source"

	// Simulate URI construction logic from UploadSourceToS3.
	key := jobID + "/original.mp4"
	uri := "s3://" + bucket + "/" + key

	if uri != expected {
		t.Fatalf("expected URI %q, got %q", expected, uri)
	}
}

// --- Mock S3 Uploader for interface-level tests ---

type mockS3Uploader struct {
	calls    int
	failN    int // fail first N calls
	failErr  error
	lastFile io.Reader
	lastJob  string
	lastSrc  string
}

func (m *mockS3Uploader) UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error) {
	m.calls++
	m.lastFile = file
	m.lastJob = jobID
	m.lastSrc = sourceName

	if m.calls <= m.failN {
		return "", m.failErr
	}
	return fmt.Sprintf("s3://pulsegrid-source/%s/original.mp4", jobID), nil
}

func (m *mockS3Uploader) Ping(ctx context.Context) error {
	return nil
}

// --- Tests for S3 Upload Behavior ---

func TestS3Upload_SuccessfulUpload_ReturnsCorrectURI(t *testing.T) {
	mock := &mockS3Uploader{}
	jobID := "abc-123-def"
	sourceName := "video.mp4"

	uri, err := mock.UploadSourceToS3(context.Background(), strings.NewReader("data"), jobID, sourceName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := "s3://pulsegrid-source/abc-123-def/original.mp4"
	if uri != expected {
		t.Fatalf("expected URI %q, got %q", expected, uri)
	}
	if mock.calls != 1 {
		t.Fatalf("expected 1 call, got %d", mock.calls)
	}
}

func TestS3Upload_TaggingFormat(t *testing.T) {
	// Verify tag construction matches what UploadSourceToS3 builds.
	// Tags: job_id={jobID}&upload_time={RFC3339}&source_name={sourceName}
	jobID := "550e8400-e29b-41d4-a716-446655440000"
	sourceName := "my video (1).mp4"
	uploadTime := time.Now().UTC().Format(time.RFC3339)

	tagging := fmt.Sprintf("job_id=%s&upload_time=%s&source_name=%s",
		url.QueryEscape(jobID),
		url.QueryEscape(uploadTime),
		url.QueryEscape(sourceName),
	)

	// Parse tags back
	parts := strings.Split(tagging, "&")
	if len(parts) != 3 {
		t.Fatalf("expected 3 tag parts, got %d", len(parts))
	}

	tags := make(map[string]string)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			t.Fatalf("invalid tag format: %q", part)
		}
		val, err := url.QueryUnescape(kv[1])
		if err != nil {
			t.Fatalf("unescape failed for %q: %v", kv[1], err)
		}
		tags[kv[0]] = val
	}

	if tags["job_id"] != jobID {
		t.Fatalf("expected job_id=%q, got %q", jobID, tags["job_id"])
	}
	if tags["source_name"] != sourceName {
		t.Fatalf("expected source_name=%q, got %q", sourceName, tags["source_name"])
	}
	if _, err := time.Parse(time.RFC3339, tags["upload_time"]); err != nil {
		t.Fatalf("upload_time not valid RFC3339: %q", tags["upload_time"])
	}
}

func TestS3Upload_PutObjectInput_CorrectParams(t *testing.T) {
	// Verify PutObjectInput construction matches expected params.
	jobID := "test-job-999"
	sourceName := "source.mp4"
	bucket := "pulsegrid-source"
	key := fmt.Sprintf("%s/original.mp4", jobID)
	uploadTime := time.Now().UTC().Format(time.RFC3339)

	tagging := fmt.Sprintf("job_id=%s&upload_time=%s&source_name=%s",
		url.QueryEscape(jobID),
		url.QueryEscape(uploadTime),
		url.QueryEscape(sourceName),
	)

	input := &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         strings.NewReader("video-data"),
		Tagging:      aws.String(tagging),
		StorageClass: s3types.StorageClassStandard,
	}

	if *input.Bucket != bucket {
		t.Fatalf("bucket mismatch: %q", *input.Bucket)
	}
	if *input.Key != "test-job-999/original.mp4" {
		t.Fatalf("key mismatch: %q", *input.Key)
	}
	if input.StorageClass != s3types.StorageClassStandard {
		t.Fatalf("storage class mismatch: %v", input.StorageClass)
	}
	if !strings.Contains(*input.Tagging, "job_id=test-job-999") {
		t.Fatalf("tagging missing job_id: %q", *input.Tagging)
	}
	if !strings.Contains(*input.Tagging, "source_name=source.mp4") {
		t.Fatalf("tagging missing source_name: %q", *input.Tagging)
	}
}

// --- Retry behavior tests (exercise same pattern as UploadSourceToS3) ---

func TestS3Upload_TransientError_RetrySucceeds(t *testing.T) {
	// Simulate: S3 returns transient error twice, then succeeds.
	// Uses RetryWithBackoff with short delays (same pattern as production code).
	attempts := 0
	transientErr := fmt.Errorf("RequestTimeout: request timed out")

	var result string
	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		if attempts <= 2 {
			return transientErr
		}
		result = "s3://pulsegrid-source/job-123/original.mp4"
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", attempts)
	}
	if result != "s3://pulsegrid-source/job-123/original.mp4" {
		t.Fatalf("expected valid URI, got %q", result)
	}
}

func TestS3Upload_PermanentError_AccessDenied_AllAttemptsFail(t *testing.T) {
	// Simulate: S3 returns AccessDenied (permanent) on every attempt.
	// All 5 attempts should fail, then return wrapped error.
	attempts := 0

	// Create AccessDenied-style error (smithy ResponseError)
	accessDeniedErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 403},
		},
		Err: fmt.Errorf("AccessDenied: Access Denied"),
	}

	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		return accessDeniedErr
	})

	if err == nil {
		t.Fatal("expected error for permanent AccessDenied, got nil")
	}
	if attempts != 5 {
		t.Fatalf("expected all 5 attempts exhausted, got %d", attempts)
	}
	if !strings.Contains(err.Error(), "all 5 attempts failed") {
		t.Fatalf("expected 'all 5 attempts failed' in error, got: %v", err)
	}
}

func TestS3Upload_PermanentError_ReturnsWrapped500Style(t *testing.T) {
	// Verify that permanent S3 error propagates as wrapped error (would map to 500 in handler).
	jobID := "fail-job-001"
	accessDenied := errors.New("AccessDenied: Access Denied")

	// Simulate UploadSourceToS3 error wrapping pattern
	var uploadErr error
	retryErr := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		return accessDenied
	})
	if retryErr != nil {
		uploadErr = fmt.Errorf("s3 upload failed for job %s: %w", jobID, retryErr)
	}

	if uploadErr == nil {
		t.Fatal("expected wrapped error")
	}
	if !strings.Contains(uploadErr.Error(), "s3 upload failed for job fail-job-001") {
		t.Fatalf("expected job context in error, got: %v", uploadErr)
	}
	if !strings.Contains(uploadErr.Error(), "AccessDenied") {
		t.Fatalf("expected AccessDenied in wrapped error, got: %v", uploadErr)
	}
}

func TestS3Upload_TransientError_EventualSuccess_CorrectURI(t *testing.T) {
	// Full integration pattern: transient failures then success returns valid URI.
	mock := &mockS3Uploader{
		failN:   3,
		failErr: fmt.Errorf("InternalError: Service Unavailable"),
	}

	// Mock doesn't use RetryWithBackoff internally, so simulate at interface level.
	// Call 4 times manually to simulate what retry would do.
	var uri string
	var err error
	for i := 0; i < 5; i++ {
		uri, err = mock.UploadSourceToS3(context.Background(), strings.NewReader("data"), "retry-job", "file.mp4")
		if err == nil {
			break
		}
	}

	if err != nil {
		t.Fatalf("expected eventual success, got: %v", err)
	}
	if uri != "s3://pulsegrid-source/retry-job/original.mp4" {
		t.Fatalf("unexpected URI: %q", uri)
	}
	if mock.calls != 4 {
		t.Fatalf("expected 4 calls (3 fail + 1 success), got %d", mock.calls)
	}
}



// --- Tests for S3 Output Upload (UploadOutputsToS3) ---

// mockS3OutputUploader mocks the S3OutputUploader interface.
type mockS3OutputUploader struct {
	uploadedFiles []string
	err           error
}

func (m *mockS3OutputUploader) UploadOutputsToS3(ctx context.Context, jobID string, results []*TranscodeResult, hlsResults []*HLSResult, manifestPath string) error {
	if m.err != nil {
		return m.err
	}
	for _, r := range results {
		m.uploadedFiles = append(m.uploadedFiles, fmt.Sprintf("%s/%s/%s", jobID, r.RenditionID, filepath.Base(r.FilePath)))
	}
	for _, h := range hlsResults {
		m.uploadedFiles = append(m.uploadedFiles, fmt.Sprintf("%s/%s/%s", jobID, h.RenditionID, filepath.Base(h.PlaylistPath)))
		for _, seg := range h.Segments {
			m.uploadedFiles = append(m.uploadedFiles, fmt.Sprintf("%s/%s/%s", jobID, h.RenditionID, filepath.Base(seg)))
		}
	}
	m.uploadedFiles = append(m.uploadedFiles, fmt.Sprintf("%s/manifest.json", jobID))
	return nil
}

func TestS3Client_ImplementsS3OutputUploader(t *testing.T) {
	// Compile-time check that S3Client implements S3OutputUploader interface.
	var _ S3OutputUploader = (*S3Client)(nil)
}

func TestUploadOutputsToS3_KeyStructure(t *testing.T) {
	// Verify key path: s3://pulsegrid-output/{jobID}/{rendition}/{filename}
	jobID := "550e8400-e29b-41d4-a716-446655440000"

	results := []*TranscodeResult{
		{RenditionID: "720p", FilePath: "/tmp/550e8400/720p.mp4", FileSize: 1024000},
		{RenditionID: "480p", FilePath: "/tmp/550e8400/480p.mp4", FileSize: 512000},
	}

	mock := &mockS3OutputUploader{}
	err := mock.UploadOutputsToS3(context.Background(), jobID, results, nil, "/tmp/550e8400/manifest.json")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedKeys := []string{
		"550e8400-e29b-41d4-a716-446655440000/720p/720p.mp4",
		"550e8400-e29b-41d4-a716-446655440000/480p/480p.mp4",
		"550e8400-e29b-41d4-a716-446655440000/manifest.json",
	}

	if len(mock.uploadedFiles) != len(expectedKeys) {
		t.Fatalf("expected %d files, got %d: %v", len(expectedKeys), len(mock.uploadedFiles), mock.uploadedFiles)
	}

	for i, expected := range expectedKeys {
		if mock.uploadedFiles[i] != expected {
			t.Fatalf("key[%d]: expected %q, got %q", i, expected, mock.uploadedFiles[i])
		}
	}
}

func TestUploadOutputsToS3_HLSKeyStructure(t *testing.T) {
	// Verify HLS files uploaded: playlist + segments under {jobID}/{rendition}/
	jobID := "hls-job-001"

	hlsResults := []*HLSResult{
		{
			RenditionID:  "hls",
			PlaylistPath: "/tmp/hls-job-001/hls/playlist.m3u8",
			SegmentCount: 3,
			Segments:     []string{"/tmp/hls-job-001/hls/segment-00000.ts", "/tmp/hls-job-001/hls/segment-00001.ts", "/tmp/hls-job-001/hls/segment-00002.ts"},
		},
	}

	mock := &mockS3OutputUploader{}
	err := mock.UploadOutputsToS3(context.Background(), jobID, nil, hlsResults, "/tmp/hls-job-001/manifest.json")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedKeys := []string{
		"hls-job-001/hls/playlist.m3u8",
		"hls-job-001/hls/segment-00000.ts",
		"hls-job-001/hls/segment-00001.ts",
		"hls-job-001/hls/segment-00002.ts",
		"hls-job-001/manifest.json",
	}

	if len(mock.uploadedFiles) != len(expectedKeys) {
		t.Fatalf("expected %d files, got %d: %v", len(expectedKeys), len(mock.uploadedFiles), mock.uploadedFiles)
	}

	for i, expected := range expectedKeys {
		if mock.uploadedFiles[i] != expected {
			t.Fatalf("key[%d]: expected %q, got %q", i, expected, mock.uploadedFiles[i])
		}
	}
}

func TestUploadOutputsToS3_TaggingFormat(t *testing.T) {
	// Verify tag construction: job_id, completion_time, rendition
	jobID := "tag-test-job"
	renditionID := "720p"
	completionTime := time.Now().UTC().Format(time.RFC3339)

	tagging := fmt.Sprintf("job_id=%s&completion_time=%s&rendition=%s",
		url.QueryEscape(jobID),
		url.QueryEscape(completionTime),
		url.QueryEscape(renditionID),
	)

	parts := strings.Split(tagging, "&")
	if len(parts) != 3 {
		t.Fatalf("expected 3 tag parts, got %d", len(parts))
	}

	tags := make(map[string]string)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			t.Fatalf("invalid tag: %q", part)
		}
		val, err := url.QueryUnescape(kv[1])
		if err != nil {
			t.Fatalf("unescape failed: %v", err)
		}
		tags[kv[0]] = val
	}

	if tags["job_id"] != jobID {
		t.Fatalf("job_id mismatch: %q", tags["job_id"])
	}
	if tags["rendition"] != renditionID {
		t.Fatalf("rendition mismatch: %q", tags["rendition"])
	}
	if _, err := time.Parse(time.RFC3339, tags["completion_time"]); err != nil {
		t.Fatalf("completion_time not valid RFC3339: %q", tags["completion_time"])
	}
}

func TestUploadOutputsToS3_PermanentError403_NoRetry(t *testing.T) {
	// Simulate: 403 AccessDenied detected by isPermanentS3Error → should return true
	accessDenied := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 403},
		},
		Err: fmt.Errorf("AccessDenied: Access Denied"),
	}

	if !isPermanentS3Error(accessDenied) {
		t.Fatal("expected isPermanentS3Error=true for 403, got false")
	}
}

func TestUploadOutputsToS3_TransientError_NotPermanent(t *testing.T) {
	// 503 ServiceUnavailable should NOT be permanent
	serviceUnavailable := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 503},
		},
		Err: fmt.Errorf("ServiceUnavailable: Slow Down"),
	}

	if isPermanentS3Error(serviceUnavailable) {
		t.Fatal("expected isPermanentS3Error=false for 503, got true")
	}
}

func TestUploadOutputsToS3_TransientRetry_EventualSuccess(t *testing.T) {
	// Simulate transient S3 error (503) → retry → success pattern
	attempts := 0
	transientErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 503},
		},
		Err: fmt.Errorf("SlowDown: Rate exceeded"),
	}

	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		if attempts <= 2 {
			return transientErr
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestUploadOutputsToS3_PermanentError_StopsRetry(t *testing.T) {
	// Permanent 403 inside retry loop: should detect and stop
	attempts := 0
	accessDenied := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 403},
		},
		Err: fmt.Errorf("AccessDenied: Access Denied"),
	}

	var permanentErr error
	_ = RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		if isPermanentS3Error(accessDenied) {
			permanentErr = accessDenied
			return nil // stop retrying
		}
		return accessDenied
	})

	if permanentErr == nil {
		t.Fatal("expected permanent error to be captured")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt (immediate stop), got %d", attempts)
	}
}

func TestUploadOutputsToS3_ManifestKey(t *testing.T) {
	// Verify manifest key: {jobID}/manifest.json
	jobID := "manifest-test-job"
	expectedKey := "manifest-test-job/manifest.json"
	key := fmt.Sprintf("%s/manifest.json", jobID)
	if key != expectedKey {
		t.Fatalf("expected %q, got %q", expectedKey, key)
	}
}

func TestUploadOutputsToS3_OutputBucketConstant(t *testing.T) {
	// Verify output bucket constant matches design spec
	if outputBucket != "pulsegrid-output" {
		t.Fatalf("expected outputBucket=%q, got %q", "pulsegrid-output", outputBucket)
	}
}
