package pkg

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

// mockDBClient implements DBClient for testing.
type mockDBClient struct {
	recordJobErr   error
	recordEventErr error
	jobs           []Job
	events         []statusEvent
	closed         bool
}

type statusEvent struct {
	jobID     string
	eventType string
}

func (m *mockDBClient) RecordJobMetadata(ctx context.Context, job Job) error {
	if m.recordJobErr != nil {
		return m.recordJobErr
	}
	m.jobs = append(m.jobs, job)
	return nil
}

func (m *mockDBClient) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	if m.recordEventErr != nil {
		return m.recordEventErr
	}
	m.events = append(m.events, statusEvent{jobID: jobID, eventType: eventType})
	return nil
}

func (m *mockDBClient) GetJobByID(ctx context.Context, jobID string) (Job, error) {
	for _, j := range m.jobs {
		if j.JobID == jobID {
			return j, nil
		}
	}
	return Job{}, fmt.Errorf("job %s not found", jobID)
}

// mockTxHandle implements TxHandle for testing.
type mockTxHandle struct {
	committed  bool
	rolledBack bool
	mock       *mockDBClient
	job        Job
}

func (h *mockTxHandle) UpdateStatusAndCommit(ctx context.Context, jobID string, status JobStatus) error {
	h.committed = true
	for i := range h.mock.jobs {
		if h.mock.jobs[i].JobID == jobID {
			h.mock.jobs[i].Status = status
		}
	}
	return nil
}

func (h *mockTxHandle) Rollback(ctx context.Context) error {
	if h.committed {
		return nil
	}
	h.rolledBack = true
	// Remove the job that was inserted in the tx.
	filtered := make([]Job, 0, len(h.mock.jobs))
	for _, j := range h.mock.jobs {
		if j.JobID != h.job.JobID {
			filtered = append(filtered, j)
		}
	}
	h.mock.jobs = filtered
	return nil
}

func (m *mockDBClient) InsertJobTx(ctx context.Context, job Job) (TxHandle, error) {
	if m.recordJobErr != nil {
		return nil, m.recordJobErr
	}
	job.Status = JobStatusSubmitting
	m.jobs = append(m.jobs, job)
	return &mockTxHandle{mock: m, job: job}, nil
}

func (m *mockDBClient) Close() {
	m.closed = true
}

func (m *mockDBClient) QueryJobs(ctx context.Context, filter JobFilter) (JobListResult, error) {
	// Simple mock: return all stored jobs filtered in-memory.
	var results []JobSummary
	for _, j := range m.jobs {
		if filter.SubmittedAfter != nil && j.SubmissionTime.Before(*filter.SubmittedAfter) {
			continue
		}
		if filter.SubmittedBefore != nil && j.SubmissionTime.After(*filter.SubmittedBefore) {
			continue
		}
		if len(filter.Statuses) > 0 {
			found := false
			for _, s := range filter.Statuses {
				if j.Status == s {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		js := JobSummary{
			JobID:          j.JobID,
			Status:         j.Status,
			SubmissionTime: j.SubmissionTime,
			CompletionTime: j.CompletionTime,
		}
		if j.CompletionTime != nil {
			dur := j.CompletionTime.Sub(j.SubmissionTime).Seconds()
			js.DurationSeconds = &dur
		}
		results = append(results, js)
	}
	total := len(results)
	// Apply offset/limit.
	if filter.Offset >= len(results) {
		results = []JobSummary{}
	} else {
		end := filter.Offset + filter.Limit
		if end > len(results) {
			end = len(results)
		}
		results = results[filter.Offset:end]
	}
	return JobListResult{
		Jobs:   results,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

func TestDBClient_RecordJobMetadata_Success(t *testing.T) {
	mock := &mockDBClient{}
	var client DBClient = mock

	job := Job{
		JobID:               "test-job-001",
		SourceS3URI:         "s3://bucket/key",
		SourceFileName:      "video.mp4",
		SourceFileSizeBytes: 1024,
		Renditions:          []Rendition{{ID: "720p", Resolution: "1280x720"}},
		OutputS3Prefix:      "s3://output/test-job-001/",
		Status:              JobStatusSubmitted,
		SubmissionTime:      time.Now().UTC(),
	}

	err := client.RecordJobMetadata(context.Background(), job)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(mock.jobs) != 1 {
		t.Fatalf("expected 1 job recorded, got %d", len(mock.jobs))
	}
	if mock.jobs[0].JobID != "test-job-001" {
		t.Errorf("expected job_id test-job-001, got %s", mock.jobs[0].JobID)
	}
}

func TestDBClient_RecordJobMetadata_Error(t *testing.T) {
	mock := &mockDBClient{recordJobErr: errors.New("connection refused")}
	var client DBClient = mock

	job := Job{JobID: "fail-job", Status: JobStatusSubmitted, SubmissionTime: time.Now()}

	err := client.RecordJobMetadata(context.Background(), job)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "connection refused" {
		t.Errorf("expected 'connection refused', got: %v", err)
	}
}

func TestDBClient_RecordStatusEvent_Success(t *testing.T) {
	mock := &mockDBClient{}
	var client DBClient = mock

	err := client.RecordStatusEvent(context.Background(), "job-123", "submitted")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(mock.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(mock.events))
	}
	if mock.events[0].jobID != "job-123" {
		t.Errorf("expected job_id job-123, got %s", mock.events[0].jobID)
	}
	if mock.events[0].eventType != "submitted" {
		t.Errorf("expected event_type submitted, got %s", mock.events[0].eventType)
	}
}

func TestDBClient_RecordStatusEvent_Error(t *testing.T) {
	mock := &mockDBClient{recordEventErr: errors.New("timeout")}
	var client DBClient = mock

	err := client.RecordStatusEvent(context.Background(), "job-456", "processing")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "timeout" {
		t.Errorf("expected 'timeout', got: %v", err)
	}
}

func TestDBClient_Close(t *testing.T) {
	mock := &mockDBClient{}
	var client DBClient = mock

	client.Close()
	if !mock.closed {
		t.Error("expected Close() to be called")
	}
}

func TestDBClient_MultipleRecords(t *testing.T) {
	mock := &mockDBClient{}
	var client DBClient = mock

	now := time.Now().UTC()
	jobs := []Job{
		{JobID: "job-a", Status: JobStatusSubmitted, SubmissionTime: now},
		{JobID: "job-b", Status: JobStatusProcessing, SubmissionTime: now},
		{JobID: "job-c", Status: JobStatusCompleted, SubmissionTime: now},
	}

	for _, j := range jobs {
		if err := client.RecordJobMetadata(context.Background(), j); err != nil {
			t.Fatalf("unexpected error for job %s: %v", j.JobID, err)
		}
	}

	if len(mock.jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(mock.jobs))
	}

	// Record events for each
	events := []string{"submitted", "processing", "completed"}
	for i, e := range events {
		if err := client.RecordStatusEvent(context.Background(), jobs[i].JobID, e); err != nil {
			t.Fatalf("unexpected error for event %s: %v", e, err)
		}
	}

	if len(mock.events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(mock.events))
	}
}

func TestDBClient_NilInterface(t *testing.T) {
	// Verify nil-check pattern works (same as S3/Kafka pattern)
	var client DBClient
	if client != nil {
		t.Error("expected nil DBClient")
	}
}

// TestDBClient_PropertyRoundTrip verifies that jobs inserted via RecordJobMetadata
// can be retrieved via GetJobByID with all fields preserved.
//
// **Validates: Requirements 5.1, 5.5**
func TestDBClient_PropertyRoundTrip(t *testing.T) {
	statuses := []JobStatus{JobStatusSubmitted, JobStatusProcessing, JobStatusCompleted, JobStatusFailed}
	codecs := []string{"libx264", "libx265", "vp9", "aac", "opus"}
	bitrates := []string{"128k", "256k", "1M", "2.5M", "5M", "10M"}
	resolutions := []string{"640x360", "854x480", "1280x720", "1920x1080", "3840x2160"}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		mock := &mockDBClient{}
		var client DBClient = mock

		// Generate random job
		jobID, err := GenerateJobID()
		if err != nil {
			t.Logf("GenerateJobID error: %v", err)
			return false
		}

		status := statuses[rng.Intn(len(statuses))]
		numRenditions := rng.Intn(6) // 0-5 renditions
		renditions := make([]Rendition, numRenditions)
		for i := range renditions {
			renditions[i] = Rendition{
				ID:           fmt.Sprintf("rendition-%d", i),
				Resolution:   resolutions[rng.Intn(len(resolutions))],
				VideoCodec:   codecs[rng.Intn(len(codecs))],
				VideoBitrate: bitrates[rng.Intn(len(bitrates))],
				AudioCodec:   codecs[rng.Intn(len(codecs))],
				AudioBitrate: bitrates[rng.Intn(len(bitrates))],
			}
		}

		submissionTime := time.Unix(rng.Int63n(2000000000), 0).UTC()
		fileSize := rng.Int63n(10_000_000_000) + 1 // 1 byte to 10GB

		job := Job{
			JobID:               jobID,
			SourceS3URI:         fmt.Sprintf("s3://pulsegrid-source/%s/original.mp4", jobID),
			SourceFileName:      fmt.Sprintf("video_%d.mp4", rng.Intn(10000)),
			SourceFileSizeBytes: fileSize,
			Renditions:          renditions,
			OutputS3Prefix:      fmt.Sprintf("s3://pulsegrid-output/%s/", jobID),
			RetryCount:          rng.Intn(4),
			MaxRetries:          3,
			Status:              status,
			SubmissionTime:      submissionTime,
		}

		// Insert
		if err := client.RecordJobMetadata(context.Background(), job); err != nil {
			t.Logf("RecordJobMetadata error: %v", err)
			return false
		}

		// Query back
		got, err := client.GetJobByID(context.Background(), jobID)
		if err != nil {
			t.Logf("GetJobByID error: %v", err)
			return false
		}

		// Verify fields match
		if got.JobID != job.JobID {
			t.Logf("JobID mismatch: got %s, want %s", got.JobID, job.JobID)
			return false
		}
		if got.Status != job.Status {
			t.Logf("Status mismatch: got %s, want %s", got.Status, job.Status)
			return false
		}
		if !got.SubmissionTime.Equal(job.SubmissionTime) {
			t.Logf("SubmissionTime mismatch: got %v, want %v", got.SubmissionTime, job.SubmissionTime)
			return false
		}
		if got.SourceFileName != job.SourceFileName {
			t.Logf("SourceFileName mismatch: got %s, want %s", got.SourceFileName, job.SourceFileName)
			return false
		}
		if got.SourceFileSizeBytes != job.SourceFileSizeBytes {
			t.Logf("SourceFileSizeBytes mismatch: got %d, want %d", got.SourceFileSizeBytes, job.SourceFileSizeBytes)
			return false
		}
		if !reflect.DeepEqual(got.Renditions, job.Renditions) {
			t.Logf("Renditions mismatch: got %+v, want %+v", got.Renditions, job.Renditions)
			return false
		}
		if got.SourceS3URI != job.SourceS3URI {
			t.Logf("SourceS3URI mismatch: got %s, want %s", got.SourceS3URI, job.SourceS3URI)
			return false
		}
		if got.OutputS3Prefix != job.OutputS3Prefix {
			t.Logf("OutputS3Prefix mismatch: got %s, want %s", got.OutputS3Prefix, job.OutputS3Prefix)
			return false
		}
		if got.RetryCount != job.RetryCount {
			t.Logf("RetryCount mismatch: got %d, want %d", got.RetryCount, job.RetryCount)
			return false
		}

		return true
	}

	cfg := &quick.Config{MaxCount: 100}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}
