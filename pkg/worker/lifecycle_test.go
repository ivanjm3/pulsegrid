package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"pulsegrid/pkg"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
)

// fakeRetryPublisher records every job passed to EnqueueJob.
type fakeRetryPublisher struct {
	jobs []pkg.Job
	err  error
}

func (f *fakeRetryPublisher) EnqueueJob(ctx context.Context, job pkg.Job) error {
	if f.err != nil {
		return f.err
	}
	f.jobs = append(f.jobs, job)
	return nil
}

// fakeDLQPublisher records every message passed to SendDLQ.
type fakeDLQPublisher struct {
	messages []queue.DLQMessage
	err      error
}

func (f *fakeDLQPublisher) SendDLQ(ctx context.Context, msg queue.DLQMessage) error {
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, msg)
	return nil
}

// fakeStatusRecorder records every event passed to RecordStatusEvent.
type fakeStatusRecorder struct {
	events []string
}

func (f *fakeStatusRecorder) RecordStatusEvent(ctx context.Context, jobID, eventType string, eventData map[string]any, podID string) error {
	f.events = append(f.events, eventType)
	return nil
}

func newTestLifecycleHandler() (*LifecycleHandler, *fakeRetryPublisher, *fakeDLQPublisher, *fakeStatusRecorder) {
	retry := &fakeRetryPublisher{}
	dlq := &fakeDLQPublisher{}
	store := &fakeStatusRecorder{}
	h := NewLifecycleHandler(retry, dlq, store, metrics.NewWorker(), "worker-pod-test")
	return h, retry, dlq, store
}

// retryableTransientError simulates a network-style transient failure that
// ClassifyError has no permanent signal for.
type retryableTransientError struct{}

func (retryableTransientError) Error() string { return "connection reset by peer" }

// TestHandleFailure_RetryCountIncrement is Property 3: Retry Count Increment
// on Failure. Validates Requirements 2.4: for any job with retry_count in
// [0, 2] and a retryable error, HandleFailure re-enqueues the job with
// retry_count incremented by exactly 1, and does not touch the DLQ.
func TestHandleFailure_RetryCountIncrement(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(1))

	for i := 0; i < iterations; i++ {
		retryCount := rnd.Intn(3) // 0, 1, or 2 — all below MaxRetries
		h, retry, dlq, _ := newTestLifecycleHandler()

		msg := queue.JobMessage{
			JobID:              fmt.Sprintf("job-%d", i),
			RetryCount:         retryCount,
			SubmittedTimestamp: "2024-01-15T10:30:00Z",
		}

		outcome, err := h.HandleFailure(context.Background(), msg, retryableTransientError{})
		if err != nil {
			t.Fatalf("retry_count=%d: HandleFailure returned error: %v", retryCount, err)
		}
		if outcome != OutcomeRetried {
			t.Fatalf("retry_count=%d: outcome = %s, want %s", retryCount, outcome, OutcomeRetried)
		}
		if len(dlq.messages) != 0 {
			t.Fatalf("retry_count=%d: DLQ received a message, want none", retryCount)
		}
		if len(retry.jobs) != 1 {
			t.Fatalf("retry_count=%d: EnqueueJob called %d times, want 1", retryCount, len(retry.jobs))
		}
		if got, want := retry.jobs[0].RetryCount, retryCount+1; got != want {
			t.Fatalf("retry_count=%d: re-enqueued retry_count = %d, want %d", retryCount, got, want)
		}
		if retry.jobs[0].ID != msg.JobID {
			t.Fatalf("re-enqueued job id = %q, want %q", retry.jobs[0].ID, msg.JobID)
		}
	}
}

// unsupportedCodecError simulates a permanent ffmpeg failure via
// *pkg.TranscodingError, matching the design doc's permanent-error examples.
func unsupportedCodecError(jobID string) error {
	return &pkg.TranscodingError{
		JobID:  jobID,
		Stderr: "[libx264 @ 0x...] unsupported codec: VP9",
		Err:    errors.New("exit status 1"),
	}
}

// TestHandleFailure_DLQOnMaxRetriesOrPermanentError is Property 4: Dead
// Letter Queue Entry on Max Retries OR Permanent Errors. Validates
// Requirements 2.5, 11.1, 11.5.
func TestHandleFailure_DLQOnMaxRetriesOrPermanentError(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(2))

	t.Run("retry_count_at_max_retryable_error", func(t *testing.T) {
		for i := 0; i < iterations; i++ {
			h, retry, dlq, _ := newTestLifecycleHandler()
			msg := queue.JobMessage{JobID: fmt.Sprintf("job-max-%d", i), RetryCount: MaxRetries}

			outcome, err := h.HandleFailure(context.Background(), msg, retryableTransientError{})
			if err != nil {
				t.Fatalf("iteration %d: HandleFailure returned error: %v", i, err)
			}
			if outcome != OutcomeDLQd {
				t.Fatalf("iteration %d: outcome = %s, want %s", i, outcome, OutcomeDLQd)
			}
			if len(retry.jobs) != 0 {
				t.Fatalf("iteration %d: job was re-enqueued, want DLQ only", i)
			}
			if len(dlq.messages) != 1 {
				t.Fatalf("iteration %d: DLQ received %d messages, want 1", i, len(dlq.messages))
			}
			m := dlq.messages[0]
			if m.JobID != msg.JobID {
				t.Fatalf("DLQ message job_id = %q, want %q", m.JobID, msg.JobID)
			}
			if m.FailureReason == "" || m.DLQEntryTimestamp == "" || m.FailureTimestamp == "" || m.PodID == "" {
				t.Fatalf("iteration %d: DLQ message missing required fields: %+v", i, m)
			}
		}
	})

	t.Run("permanent_error_immediate_dlq_regardless_of_retry_count", func(t *testing.T) {
		for i := 0; i < iterations; i++ {
			retryCount := rnd.Intn(4) // 0..3, including 0 (never retried yet)
			h, retry, dlq, _ := newTestLifecycleHandler()
			msg := queue.JobMessage{JobID: fmt.Sprintf("job-perm-%d", i), RetryCount: retryCount}

			outcome, err := h.HandleFailure(context.Background(), msg, unsupportedCodecError(msg.JobID))
			if err != nil {
				t.Fatalf("retry_count=%d: HandleFailure returned error: %v", retryCount, err)
			}
			if outcome != OutcomeDLQd {
				t.Fatalf("retry_count=%d: outcome = %s, want %s (permanent errors skip retry)", retryCount, outcome, OutcomeDLQd)
			}
			if len(retry.jobs) != 0 {
				t.Fatalf("retry_count=%d: job was re-enqueued, want immediate DLQ (no retry)", retryCount)
			}
			if len(dlq.messages) != 1 {
				t.Fatalf("retry_count=%d: DLQ received %d messages, want 1", retryCount, len(dlq.messages))
			}
			if dlq.messages[0].StderrSnippet == "" {
				t.Fatalf("retry_count=%d: DLQ message missing stderr_snippet for ffmpeg failure", retryCount)
			}
		}
	})
}

func TestHandleFailure_ResourceConstraint_DoesNotRetryOrDLQ(t *testing.T) {
	h, retry, dlq, store := newTestLifecycleHandler()
	msg := queue.JobMessage{JobID: "job-oom", RetryCount: 0}

	outcome, err := h.HandleFailure(context.Background(), msg, &pkg.ResourceConstraintError{Resource: "disk", Err: errors.New("no space left on device")})
	if err != nil {
		t.Fatalf("HandleFailure returned error: %v", err)
	}
	if outcome != OutcomeConstrained {
		t.Fatalf("outcome = %s, want %s", outcome, OutcomeConstrained)
	}
	if len(retry.jobs) != 0 || len(dlq.messages) != 0 {
		t.Fatalf("resource constraint must not retry or DLQ: retry=%d dlq=%d", len(retry.jobs), len(dlq.messages))
	}
	if len(store.events) != 1 || store.events[0] != "pod_resource_constrained" {
		t.Fatalf("status events = %v, want [pod_resource_constrained]", store.events)
	}
}

func TestHandleSuccess_RecordsCompletion(t *testing.T) {
	h, _, _, store := newTestLifecycleHandler()
	if err := h.HandleSuccess(context.Background(), queue.JobMessage{JobID: "job-1"}); err != nil {
		t.Fatalf("HandleSuccess returned error: %v", err)
	}
	if len(store.events) != 1 || store.events[0] != "job_completed" {
		t.Fatalf("status events = %v, want [job_completed]", store.events)
	}
}

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"resource constraint", &pkg.ResourceConstraintError{Resource: "disk", Err: errors.New("oom")}, ErrorClassConstraint},
		{"unsupported codec", unsupportedCodecError("job-1"), ErrorClassPermanent},
		{"source not found", errors.New("download source from s3: source object not found: x"), ErrorClassPermanent},
		{"transient network error", retryableTransientError{}, ErrorClassRetryable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("ClassifyError(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}
