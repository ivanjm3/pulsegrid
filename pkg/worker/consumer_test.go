package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg/queue"
)

// fakeReader is a MessageReader backed by an in-memory slice of messages.
// FetchMessage blocks (respecting ctx) once the slice is exhausted, mirroring
// a real Kafka reader waiting for new data — this lets tests exercise
// cancellation of a pending poll.
type fakeReader struct {
	mu        sync.Mutex
	messages  []kafka.Message
	pos       int
	committed []kafka.Message
	closed    bool
	closeErr  error
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	for {
		f.mu.Lock()
		if f.pos < len(f.messages) {
			msg := f.messages[f.pos]
			f.pos++
			f.mu.Unlock()
			return msg, nil
		}
		f.mu.Unlock()

		select {
		case <-ctx.Done():
			return kafka.Message{}, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakeReader) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

func (f *fakeReader) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// blockingHandler lets tests control exactly when HandleJob returns, so a
// job can be made to still be "in flight" when a shutdown signal fires.
type blockingHandler struct {
	release chan struct{}
	started chan struct{}
	err     error
	calls   int
	mu      sync.Mutex
}

func (h *blockingHandler) HandleJob(ctx context.Context, msg queue.JobMessage) error {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	if h.started != nil {
		close(h.started)
	}
	if h.release != nil {
		<-h.release
	}
	return h.err
}

func (h *blockingHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func jobMessageBytes(t *testing.T, jobID string) []byte {
	t.Helper()
	body, err := json.Marshal(queue.JobMessage{JobID: jobID})
	if err != nil {
		t.Fatalf("marshal job message: %v", err)
	}
	return body
}

func TestConsumer_FetchesAndProcessesMessage(t *testing.T) {
	reader := &fakeReader{
		messages: []kafka.Message{{Partition: 0, Offset: 0, Value: jobMessageBytes(t, "job-1")}},
	}
	handler := &blockingHandler{}
	c := NewConsumer(reader, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for handler.callCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("handler was never invoked")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !reader.isClosed() {
		t.Fatal("reader was not closed")
	}
}

func TestConsumer_CommitsOffsetOnlyAfterSuccessfulProcess(t *testing.T) {
	reader := &fakeReader{
		messages: []kafka.Message{{Partition: 0, Offset: 0, Value: jobMessageBytes(t, "job-1")}},
	}
	handler := &blockingHandler{}
	c := NewConsumer(reader, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for reader.committedCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("offset was never committed")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

func TestConsumer_CrashWithoutCommit_OffsetNotAdvanced(t *testing.T) {
	// A handler error simulates a crash/failure before commit: the message
	// is never marked processed, so on rebalance a fresh consumer group
	// member would re-read the same offset (there is nothing to advance
	// past).
	reader := &fakeReader{
		messages: []kafka.Message{{Partition: 0, Offset: 0, Value: jobMessageBytes(t, "job-1")}},
	}
	handler := &blockingHandler{err: errors.New("boom")}
	c := NewConsumer(reader, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for handler.callCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("handler was never invoked")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// Give the loop a moment to (not) commit before we cancel.
	time.Sleep(20 * time.Millisecond)

	cancel()
	<-done

	if got := reader.committedCount(); got != 0 {
		t.Fatalf("committed count = %d, want 0 (offset must not advance on failure)", got)
	}
}

func TestConsumer_SIGTERM_InFlightJobCompletesBeforeClose(t *testing.T) {
	reader := &fakeReader{
		messages: []kafka.Message{{Partition: 0, Offset: 0, Value: jobMessageBytes(t, "job-1")}},
	}
	handler := &blockingHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	c := NewConsumer(reader, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// Simulate SIGTERM arriving while the job is still processing.
	cancel()

	// The reader must not be closed yet — the in-flight job has not
	// finished, so Close (and loop exit) must wait.
	time.Sleep(20 * time.Millisecond)
	if reader.isClosed() {
		t.Fatal("reader closed before in-flight job completed")
	}

	close(handler.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after in-flight job completed")
	}

	if !reader.isClosed() {
		t.Fatal("reader was not closed after shutdown")
	}
	if reader.committedCount() != 1 {
		t.Fatalf("committed count = %d, want 1 (successful in-flight job should still commit)", reader.committedCount())
	}
}

func TestConsumer_JoinsGroupAndFetchesFromPartition(t *testing.T) {
	reader := &fakeReader{
		messages: []kafka.Message{
			{Partition: 2, Offset: 41, Value: jobMessageBytes(t, "job-a")},
		},
	}
	var receivedPartition int
	handler := &recordingHandler{fn: func(ctx context.Context, msg queue.JobMessage) error {
		receivedPartition = 2
		return nil
	}}
	c := NewConsumer(reader, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for reader.committedCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("message from assigned partition was never processed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if receivedPartition != 2 {
		t.Fatalf("did not observe message from partition 2")
	}
}

type recordingHandler struct {
	fn func(ctx context.Context, msg queue.JobMessage) error
}

func (h *recordingHandler) HandleJob(ctx context.Context, msg queue.JobMessage) error {
	return h.fn(ctx, msg)
}
