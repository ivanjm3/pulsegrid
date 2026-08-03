package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"pulsegrid/pkg"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
)

// fakeEventPublisher marshals every published event to JSON, the same as the
// real queue.LifecycleProducer would, so the property test below validates
// the actual wire schema rather than the in-memory struct.
type fakeEventPublisher struct {
	published [][]byte
}

func (f *fakeEventPublisher) PublishEvent(ctx context.Context, event queue.JobLifecycleEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f.published = append(f.published, body)
	return nil
}

// TestLifecycleEventSchema is Property 11: Job Lifecycle Event Schema.
// Validates Requirements 19.1.
func TestLifecycleEventSchema(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(11))

	validEventTypes := map[string]bool{
		"job_started":         true,
		"rendition_completed": true,
		"job_completed":       true,
		"job_failed":          true,
	}

	failureErrors := []error{
		retryableTransientError{},
		unsupportedCodecError("job-schema"),
		&pkg.ResourceConstraintError{Resource: "disk", Err: errors.New("no space left on device")},
	}

	for i := 0; i < iterations; i++ {
		events := &fakeEventPublisher{}
		h := NewLifecycleHandler(&fakeRetryPublisher{}, &fakeDLQPublisher{}, &fakeStatusRecorder{}, metrics.NewWorker(), "worker-pod-schema-test", NewLogger(io.Discard), events)
		jobID := fmt.Sprintf("job-schema-%d", i)

		switch rnd.Intn(4) {
		case 0: // job_started
			_ = h.HandleStart(context.Background(), jobID)
		case 1: // rendition_completed, one event per rendition
			n := 1 + rnd.Intn(5)
			for j := 0; j < n; j++ {
				h.HandleRenditionCompleted(context.Background(), jobID, fmt.Sprintf("rendition-%d", j))
			}
		case 2: // job_completed
			_ = h.HandleSuccess(context.Background(), queue.JobMessage{JobID: jobID})
		case 3: // job_failed, varied error_class (retryable, permanent, constraint)
			retryCount := rnd.Intn(4)
			procErr := failureErrors[rnd.Intn(len(failureErrors))]
			_, _ = h.HandleFailure(context.Background(), queue.JobMessage{JobID: jobID, RetryCount: retryCount}, procErr)
		}

		if len(events.published) == 0 {
			t.Fatalf("iteration %d: no lifecycle event published", i)
		}

		for _, body := range events.published {
			var decoded map[string]any
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("iteration %d: invalid JSON: %v", i, err)
			}

			if got, _ := decoded["job_id"].(string); got != jobID {
				t.Fatalf("iteration %d: job_id = %q, want %q", i, got, jobID)
			}

			eventType, _ := decoded["event_type"].(string)
			if !validEventTypes[eventType] {
				t.Fatalf("iteration %d: invalid event_type %q", i, eventType)
			}

			podID, _ := decoded["pod_id"].(string)
			if podID == "" {
				t.Fatalf("iteration %d: pod_id empty", i)
			}

			ts, _ := decoded["timestamp"].(string)
			if _, err := time.Parse(time.RFC3339, ts); err != nil {
				t.Fatalf("iteration %d: invalid timestamp %q: %v", i, ts, err)
			}

			renditionID, hasRendition := decoded["rendition_id"]
			if eventType == "rendition_completed" {
				if !hasRendition || renditionID == nil || renditionID == "" {
					t.Fatalf("iteration %d: rendition_completed missing rendition_id", i)
				}
			} else if hasRendition && renditionID != nil {
				t.Fatalf("iteration %d: %s must have null rendition_id, got %v", i, eventType, renditionID)
			}

			errorClass, hasErrorClass := decoded["error_class"]
			if eventType == "job_failed" {
				if !hasErrorClass || errorClass == nil || errorClass == "" {
					t.Fatalf("iteration %d: job_failed missing error_class", i)
				}
			} else if hasErrorClass && errorClass != nil {
				t.Fatalf("iteration %d: %s must have null error_class, got %v", i, eventType, errorClass)
			}
		}
	}
}
