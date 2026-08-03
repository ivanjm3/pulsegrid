package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// TestLogJobError_RequiredFieldsPresent is Property 10: Error Logging
// Context. Validates Requirements 3.5, 12.2: for any job processing failure,
// the structured JSON log line contains an RFC 3339 timestamp, job_id,
// pod_id, error_message, and event_type, plus retry_count and error_type;
// ffmpeg stderr is truncated to at most 500 characters.
func TestLogJobError_RequiredFieldsPresent(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(3))

	errorClasses := []ErrorClass{ErrorClassRetryable, ErrorClassPermanent, ErrorClassConstraint}
	eventTypes := []string{"job_failed", "pod_resource_constrained"}

	for i := 0; i < iterations; i++ {
		var buf bytes.Buffer
		logger := NewLogger(&buf)

		jobID := fmt.Sprintf("job-%d", rnd.Int())
		podID := fmt.Sprintf("pod-%d", rnd.Intn(50))
		retryCount := rnd.Intn(4)
		class := errorClasses[rnd.Intn(len(errorClasses))]
		eventType := eventTypes[rnd.Intn(len(eventTypes))]
		errMsg := fmt.Sprintf("simulated failure %d: connection reset", rnd.Int())
		procErr := errors.New(errMsg)

		// Roughly a third of iterations exercise the >500 char truncation
		// path for captured ffmpeg stderr.
		var stderr string
		if rnd.Intn(3) == 0 {
			stderr = strings.Repeat("x", 200+rnd.Intn(800))
		} else {
			stderr = strings.Repeat("e", rnd.Intn(400))
		}

		LogJobError(logger, eventType, jobID, podID, procErr, retryCount, class, stderr)

		var record map[string]any
		if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
			t.Fatalf("iteration %d: log line is not valid JSON: %v\nraw: %s", i, err, buf.String())
		}

		for _, field := range []string{"time", "job_id", "pod_id", "error_message", "event_type", "retry_count", "error_type", "ffmpeg_stderr"} {
			if _, ok := record[field]; !ok {
				t.Fatalf("iteration %d: log record missing field %q: %v", i, field, record)
			}
		}

		if ts, ok := record["time"].(string); !ok || ts == "" {
			t.Fatalf("iteration %d: time field not a non-empty string: %v", i, record["time"])
		} else if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("iteration %d: time field %q is not valid RFC 3339: %v", i, ts, err)
		}

		if got := record["job_id"]; got != jobID {
			t.Fatalf("iteration %d: job_id = %v, want %q", i, got, jobID)
		}
		if got := record["pod_id"]; got != podID {
			t.Fatalf("iteration %d: pod_id = %v, want %q", i, got, podID)
		}
		if got := record["error_message"]; got != errMsg {
			t.Fatalf("iteration %d: error_message = %v, want %q", i, got, errMsg)
		}
		if got := record["event_type"]; got != eventType {
			t.Fatalf("iteration %d: event_type = %v, want %q", i, got, eventType)
		}
		if got, want := record["retry_count"], float64(retryCount); got != want {
			t.Fatalf("iteration %d: retry_count = %v, want %v", i, got, want)
		}
		if got := record["error_type"]; got != string(class) {
			t.Fatalf("iteration %d: error_type = %v, want %q", i, got, class)
		}

		gotStderr, _ := record["ffmpeg_stderr"].(string)
		wantLen := len(stderr)
		if wantLen > 500 {
			wantLen = 500
		}
		if len(gotStderr) != wantLen {
			t.Fatalf("iteration %d: ffmpeg_stderr length = %d, want %d (truncated to 500 max)", i, len(gotStderr), wantLen)
		}
	}
}
