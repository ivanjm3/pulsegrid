package analytics

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

// fakeLifecycleReader is a Reader backed by an in-memory slice of messages,
// same shape as pkg/worker's fakeReader.
type fakeLifecycleReader struct {
	mu        sync.Mutex
	messages  []kafka.Message
	pos       int
	committed []kafka.Message
	closed    bool
}

func (f *fakeLifecycleReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
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

func (f *fakeLifecycleReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeLifecycleReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeLifecycleReader) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

// fakeHandler is an EventHandler that always fails if err is set, otherwise
// succeeds, recording every event it was called with.
type fakeHandler struct {
	mu     sync.Mutex
	err    error
	events []queue.JobLifecycleEvent
}

func (h *fakeHandler) HandleEvent(ctx context.Context, event queue.JobLifecycleEvent) error {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.mu.Unlock()
	return h.err
}

func (h *fakeHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.events)
}

// fakeMetrics records every call Consumer makes, so tests can assert
// counter/gauge values without a real Prometheus registry.
type fakeMetrics struct {
	mu              sync.Mutex
	eventsProcessed map[string]int
	lastSinkLag     float64
	sinkLagSet      bool
	consumerErrors  map[string]int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		eventsProcessed: make(map[string]int),
		consumerErrors:  make(map[string]int),
	}
}

func (m *fakeMetrics) IncEventsProcessed(eventType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventsProcessed[eventType]++
}

func (m *fakeMetrics) SetSinkLag(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSinkLag = seconds
	m.sinkLagSet = true
}

func (m *fakeMetrics) IncConsumerError(errorType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerErrors[errorType]++
}

func (m *fakeMetrics) totalEventsProcessed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, n := range m.eventsProcessed {
		total += n
	}
	return total
}

func (m *fakeMetrics) countFor(eventType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.eventsProcessed[eventType]
}

func (m *fakeMetrics) errorCountFor(errorType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumerErrors[errorType]
}

func lifecycleEventBytes(t *testing.T, event queue.JobLifecycleEvent) []byte {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal lifecycle event: %v", err)
	}
	return body
}

func TestConsumer_ProcessesVariedEvents_MetricsMatchCounts(t *testing.T) {
	eventTypes := []queue.LifecycleEventType{
		queue.EventJobStarted, queue.EventJobStarted, queue.EventJobStarted,
		queue.EventRenditionCompleted, queue.EventRenditionCompleted, queue.EventRenditionCompleted, queue.EventRenditionCompleted,
		queue.EventJobCompleted, queue.EventJobCompleted,
		queue.EventJobFailed,
	}
	var messages []kafka.Message
	for i, et := range eventTypes {
		messages = append(messages, kafka.Message{
			Partition: 0,
			Offset:    int64(i),
			Value: lifecycleEventBytes(t, queue.JobLifecycleEvent{
				JobID:     "job-1",
				EventType: et,
				PodID:     "worker-abc",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}),
		})
	}

	reader := &fakeLifecycleReader{messages: messages}
	handler := &fakeHandler{}
	m := newFakeMetrics()
	c := NewConsumer(reader, handler, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for handler.callCount() < len(eventTypes) {
		select {
		case <-deadline:
			t.Fatalf("only %d of %d events processed", handler.callCount(), len(eventTypes))
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if got := m.countFor(string(queue.EventJobStarted)); got != 3 {
		t.Errorf("job_started count = %d, want 3", got)
	}
	if got := m.countFor(string(queue.EventRenditionCompleted)); got != 4 {
		t.Errorf("rendition_completed count = %d, want 4", got)
	}
	if got := m.countFor(string(queue.EventJobCompleted)); got != 2 {
		t.Errorf("job_completed count = %d, want 2", got)
	}
	if got := m.countFor(string(queue.EventJobFailed)); got != 1 {
		t.Errorf("job_failed count = %d, want 1", got)
	}
	if got := m.totalEventsProcessed(); got != len(eventTypes) {
		t.Errorf("total events processed = %d, want %d", got, len(eventTypes))
	}
}

func TestConsumer_SinkFailure_ErrorCounterIncremented_OffsetNotCommitted(t *testing.T) {
	reader := &fakeLifecycleReader{
		messages: []kafka.Message{{
			Partition: 0, Offset: 0,
			Value: lifecycleEventBytes(t, queue.JobLifecycleEvent{
				JobID: "job-1", EventType: queue.EventJobStarted, PodID: "worker-abc",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}),
		}},
	}
	handler := &fakeHandler{err: errors.New("db connection reset")}
	m := newFakeMetrics()
	c := NewConsumer(reader, handler, m)

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
	time.Sleep(20 * time.Millisecond) // give the loop a moment to (not) commit
	cancel()
	<-done

	if got := m.errorCountFor("sink_write_failure"); got != 1 {
		t.Errorf("sink_write_failure count = %d, want 1", got)
	}
	if got := reader.committedCount(); got != 0 {
		t.Errorf("committed count = %d, want 0 (offset must not advance on sink failure)", got)
	}
	if m.totalEventsProcessed() != 0 {
		t.Errorf("events_processed should not increment on a failed sink write")
	}
}

func TestConsumer_ParseError_IncrementsParseErrorCounter(t *testing.T) {
	reader := &fakeLifecycleReader{
		messages: []kafka.Message{{Partition: 0, Offset: 0, Value: []byte("not valid json")}},
	}
	handler := &fakeHandler{}
	m := newFakeMetrics()
	c := NewConsumer(reader, handler, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for m.errorCountFor("parse_error") < 1 {
		select {
		case <-deadline:
			t.Fatal("parse_error was never recorded")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if handler.callCount() != 0 {
		t.Error("handler should never be invoked for an unparseable message")
	}
}

func TestConsumer_SinkLagGauge_WithinOneSecondOfActualDelta(t *testing.T) {
	eventTime := time.Now().UTC().Add(-3 * time.Second)
	reader := &fakeLifecycleReader{
		messages: []kafka.Message{{
			Partition: 0, Offset: 0,
			Value: lifecycleEventBytes(t, queue.JobLifecycleEvent{
				JobID: "job-1", EventType: queue.EventJobStarted, PodID: "worker-abc",
				Timestamp: eventTime.Format(time.RFC3339),
			}),
		}},
	}
	handler := &fakeHandler{}
	m := newFakeMetrics()
	c := NewConsumer(reader, handler, m)

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
	<-done

	wantLag := time.Since(eventTime).Seconds()
	m.mu.Lock()
	gotLag, set := m.lastSinkLag, m.sinkLagSet
	m.mu.Unlock()

	if !set {
		t.Fatal("sink lag gauge was never set")
	}
	if diff := gotLag - wantLag; diff > 1.0 || diff < -1.0 {
		t.Errorf("sink lag = %v, want within 1s of %v", gotLag, wantLag)
	}
}

func TestConsumer_NilMetrics_DoesNotPanic(t *testing.T) {
	reader := &fakeLifecycleReader{
		messages: []kafka.Message{{
			Partition: 0, Offset: 0,
			Value: lifecycleEventBytes(t, queue.JobLifecycleEvent{
				JobID: "job-1", EventType: queue.EventJobStarted, PodID: "worker-abc",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}),
		}},
	}
	handler := &fakeHandler{}
	c := NewConsumer(reader, handler, nil)

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
	<-done
}
