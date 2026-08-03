package queue

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg"
)

// fakeWriter implements Writer. writeFn drives WriteMessages behavior per
// call; captured records every message handed to it.
type fakeWriter struct {
	writeFn func(callNum int) error
	calls   int
	records []kafka.Message
}

func (f *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.calls++
	f.records = append(f.records, msgs...)
	return f.writeFn(f.calls)
}

func (f *fakeWriter) Close() error { return nil }

var codecs = []string{"libx264", "libx265", "vp9"}

func randomRendition(r *rand.Rand, i int) pkg.Rendition {
	if r.Intn(4) == 0 {
		return pkg.Rendition{ID: "hls", HLS: true}
	}
	return pkg.Rendition{
		ID:          "rendition-" + string(rune('a'+i)),
		Codec:       codecs[r.Intn(len(codecs))],
		BitrateKbps: 100 + r.Intn(8000),
		Width:       [3]int{1280, 854, 640}[r.Intn(3)],
		Height:      [3]int{720, 480, 360}[r.Intn(3)],
	}
}

func randomJob(r *rand.Rand) pkg.Job {
	n := r.Intn(6) // 0-5 renditions
	renditions := make([]pkg.Rendition, n)
	for i := 0; i < n; i++ {
		renditions[i] = randomRendition(r, i)
	}
	id, err := pkg.NewJobID()
	if err != nil {
		panic(err)
	}
	return pkg.Job{
		ID:             id,
		SourceS3URI:    "s3://pulsegrid-source/" + id + "/original.mp4",
		OutputS3Prefix: "s3://pulsegrid-output/" + id + "/",
		Renditions:     renditions,
		RetryCount:     r.Intn(3),
		SubmissionTime: time.Now().UTC(),
	}
}

// TestEnqueueJob_MessageSchemaCompliance is Property 2: for any valid job
// with 0-5 renditions of varied codecs/bitrates, the published Kafka message
// SHALL contain all required fields (job_id, source_s3_uri, renditions,
// output_s3_prefix, retry_count) with correct types and no missing fields.
func TestEnqueueJob_MessageSchemaCompliance(t *testing.T) {
	r := rand.New(rand.NewSource(1))

	for i := 0; i < 100; i++ {
		job := randomJob(r)

		fw := &fakeWriter{writeFn: func(int) error { return nil }}
		p := NewProducer(fw)
		p.sleep = func(time.Duration) {}

		if err := p.EnqueueJob(context.Background(), job); err != nil {
			t.Fatalf("iteration %d: EnqueueJob returned error: %v", i, err)
		}
		if len(fw.records) != 1 {
			t.Fatalf("iteration %d: writer got %d messages, want 1", i, len(fw.records))
		}
		rec := fw.records[0]

		if string(rec.Key) != job.ID {
			t.Fatalf("iteration %d: partition key = %q, want job_id %q", i, rec.Key, job.ID)
		}

		var decoded map[string]interface{}
		if err := json.Unmarshal(rec.Value, &decoded); err != nil {
			t.Fatalf("iteration %d: message is not valid JSON: %v", i, err)
		}

		for _, field := range []string{"job_id", "source_s3_uri", "renditions", "output_s3_prefix", "retry_count", "max_retries", "submitted_timestamp", "visibility_timeout_seconds"} {
			if _, ok := decoded[field]; !ok {
				t.Fatalf("iteration %d: message missing required field %q: %s", i, field, rec.Value)
			}
		}

		if got, ok := decoded["job_id"].(string); !ok || got != job.ID {
			t.Fatalf("iteration %d: job_id = %v, want %q", i, decoded["job_id"], job.ID)
		}
		if got, ok := decoded["source_s3_uri"].(string); !ok || got != job.SourceS3URI {
			t.Fatalf("iteration %d: source_s3_uri = %v, want %q", i, decoded["source_s3_uri"], job.SourceS3URI)
		}
		if got, ok := decoded["output_s3_prefix"].(string); !ok || got != job.OutputS3Prefix {
			t.Fatalf("iteration %d: output_s3_prefix = %v, want %q", i, decoded["output_s3_prefix"], job.OutputS3Prefix)
		}
		renditionsOut, ok := decoded["renditions"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: renditions is not an array: %v", i, decoded["renditions"])
		}
		if len(renditionsOut) != len(job.Renditions) {
			t.Fatalf("iteration %d: renditions count = %d, want %d", i, len(renditionsOut), len(job.Renditions))
		}
		retryCount, ok := decoded["retry_count"].(float64)
		if !ok || int(retryCount) != job.RetryCount {
			t.Fatalf("iteration %d: retry_count = %v, want %d", i, decoded["retry_count"], job.RetryCount)
		}
		if _, err := time.Parse(time.RFC3339, decoded["submitted_timestamp"].(string)); err != nil {
			t.Fatalf("iteration %d: submitted_timestamp not RFC3339: %v", i, err)
		}
	}
}

func TestEnqueueJob_TransientError_RetriesWithBackoff(t *testing.T) {
	fw := &fakeWriter{
		writeFn: func(callNum int) error {
			if callNum < 3 {
				return errors.New("leader not available")
			}
			return nil
		},
	}
	p := NewProducer(fw)

	var sleeps []time.Duration
	p.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }

	job := randomJob(rand.New(rand.NewSource(2)))
	if err := p.EnqueueJob(context.Background(), job); err != nil {
		t.Fatalf("EnqueueJob returned error: %v", err)
	}
	if fw.calls != 3 {
		t.Fatalf("WriteMessages called %d times, want 3", fw.calls)
	}
	want := []time.Duration{500 * time.Millisecond, 1 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
	for i := range want {
		if sleeps[i] != want[i] {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], want[i])
		}
	}
}

// fakeQueueReader implements Reader by reading directly off a fakeWriter's
// captured records, simulating a Kafka topic backed by an in-memory slice:
// FetchMessage returns the next unread record in publish order,
// CommitMessages just records which offsets were acknowledged.
type fakeQueueReader struct {
	source    *fakeWriter
	next      int
	committed []int64
}

func (f *fakeQueueReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.next >= len(f.source.records) {
		return kafka.Message{}, errors.New("fakeQueueReader: no more messages")
	}
	msg := f.source.records[f.next]
	msg.Offset = int64(f.next)
	f.next++
	return msg, nil
}

func (f *fakeQueueReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		f.committed = append(f.committed, m.Offset)
	}
	return nil
}

func (f *fakeQueueReader) Close() error { return nil }

// TestMessageQueue_PublishConsumeSchemaRoundTrip is Property 2 (integrated,
// task 23.1): for any valid job with 0-5 renditions of varied
// codecs/bitrates/resolutions, publishing through MessageQueue.Publish and
// reading it back through MessageQueue.Consume SHALL produce a JobMessage
// with all required fields present, correctly typed, and equal to what was
// published — verified end-to-end through the KafkaQueue abstraction rather
// than by inspecting the raw Kafka message directly (see
// TestEnqueueJob_MessageSchemaCompliance for that lower-level check).
func TestMessageQueue_PublishConsumeSchemaRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	fw := &fakeWriter{writeFn: func(int) error { return nil }}
	producer := NewProducer(fw)
	producer.sleep = func(time.Duration) {}
	reader := &fakeQueueReader{source: fw}
	q := NewKafkaQueue(producer, reader, nil)

	jobs := make([]pkg.Job, 100)
	for i := range jobs {
		jobs[i] = randomJob(r)
		if err := q.Publish(context.Background(), jobs[i]); err != nil {
			t.Fatalf("iteration %d: Publish returned error: %v", i, err)
		}
	}

	for i, job := range jobs {
		consumed, err := q.Consume(context.Background())
		if err != nil {
			t.Fatalf("iteration %d: Consume returned error: %v", i, err)
		}

		got := consumed.Job
		if got.JobID != job.ID {
			t.Fatalf("iteration %d: job_id = %q, want %q", i, got.JobID, job.ID)
		}
		if got.SourceS3URI != job.SourceS3URI {
			t.Fatalf("iteration %d: source_s3_uri = %q, want %q", i, got.SourceS3URI, job.SourceS3URI)
		}
		if got.OutputS3Prefix != job.OutputS3Prefix {
			t.Fatalf("iteration %d: output_s3_prefix = %q, want %q", i, got.OutputS3Prefix, job.OutputS3Prefix)
		}
		if got.RetryCount != job.RetryCount {
			t.Fatalf("iteration %d: retry_count = %d, want %d", i, got.RetryCount, job.RetryCount)
		}
		if got.MaxRetries != DefaultMaxRetries {
			t.Fatalf("iteration %d: max_retries = %d, want %d", i, got.MaxRetries, DefaultMaxRetries)
		}
		if got.VisibilityTimeoutSeconds != DefaultVisibilityTimeoutSeconds {
			t.Fatalf("iteration %d: visibility_timeout_seconds = %d, want %d", i, got.VisibilityTimeoutSeconds, DefaultVisibilityTimeoutSeconds)
		}
		if _, err := time.Parse(time.RFC3339, got.SubmittedTimestamp); err != nil {
			t.Fatalf("iteration %d: submitted_timestamp not RFC3339: %v", i, err)
		}
		if len(got.Renditions) != len(job.Renditions) {
			t.Fatalf("iteration %d: renditions count = %d, want %d", i, len(got.Renditions), len(job.Renditions))
		}
		for j, rend := range job.Renditions {
			if got.Renditions[j] != rend {
				t.Fatalf("iteration %d rendition %d: got %+v, want %+v", i, j, got.Renditions[j], rend)
			}
		}

		if err := q.Commit(context.Background(), consumed); err != nil {
			t.Fatalf("iteration %d: Commit returned error: %v", i, err)
		}
	}

	if len(reader.committed) != len(jobs) {
		t.Fatalf("committed %d offsets, want %d", len(reader.committed), len(jobs))
	}
	for i, off := range reader.committed {
		if off != int64(i) {
			t.Errorf("committed[%d] = %d, want %d", i, off, i)
		}
	}
}

func TestEnqueueJob_AllAttemptsFail_ReturnsError(t *testing.T) {
	fw := &fakeWriter{writeFn: func(int) error { return errors.New("broker down") }}
	p := NewProducer(fw)
	p.sleep = func(time.Duration) {}

	job := randomJob(rand.New(rand.NewSource(3)))
	err := p.EnqueueJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if fw.calls != maxPublishAttempts {
		t.Fatalf("WriteMessages called %d times, want %d", fw.calls, maxPublishAttempts)
	}
}
