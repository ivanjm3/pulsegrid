package queue

import (
	"context"
	"testing"
	"time"

	"pulsegrid/pkg"
)

// mockQueue implements MessageQueue for testing consumers of the interface.
type mockQueue struct {
	published  []pkg.KafkaMessage
	dlqMessages []pkg.KafkaMessage
	dlqReasons []string
	messages   []*Message
	commitLog  []*Message
	consumeIdx int
	publishErr error
	consumeErr error
	commitErr  error
	dlqErr     error
}

func (m *mockQueue) Publish(ctx context.Context, msg pkg.KafkaMessage) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, msg)
	return nil
}

func (m *mockQueue) Consume(ctx context.Context) (*Message, error) {
	if m.consumeErr != nil {
		return nil, m.consumeErr
	}
	if m.consumeIdx >= len(m.messages) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	msg := m.messages[m.consumeIdx]
	m.consumeIdx++
	return msg, nil
}

func (m *mockQueue) Commit(ctx context.Context, msg *Message) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	m.commitLog = append(m.commitLog, msg)
	return nil
}

func (m *mockQueue) SendDLQ(ctx context.Context, msg pkg.KafkaMessage, reason string) error {
	if m.dlqErr != nil {
		return m.dlqErr
	}
	m.dlqMessages = append(m.dlqMessages, msg)
	m.dlqReasons = append(m.dlqReasons, reason)
	return nil
}

func (m *mockQueue) Close() error { return nil }

// TestMockQueueImplementsInterface verifies mockQueue satisfies MessageQueue.
func TestMockQueueImplementsInterface(t *testing.T) {
	var _ MessageQueue = (*mockQueue)(nil)
}

// TestKafkaQueueImplementsInterface verifies KafkaQueue satisfies MessageQueue.
func TestKafkaQueueImplementsInterface(t *testing.T) {
	var _ MessageQueue = (*KafkaQueue)(nil)
}

// TestConfigDefaults verifies zero-value config fields get production defaults.
func TestConfigDefaults(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	}
	cfg.defaults()

	if cfg.SessionTimeout != 30*time.Minute {
		t.Errorf("expected SessionTimeout 30min, got %v", cfg.SessionTimeout)
	}
	if cfg.HeartbeatInterval != 3*time.Second {
		t.Errorf("expected HeartbeatInterval 3s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.MaxWait != 5*time.Second {
		t.Errorf("expected MaxWait 5s, got %v", cfg.MaxWait)
	}
	if cfg.RetryAttempts != 5 {
		t.Errorf("expected RetryAttempts 5, got %d", cfg.RetryAttempts)
	}
	if cfg.RetryBaseDelay != 1*time.Second {
		t.Errorf("expected RetryBaseDelay 1s, got %v", cfg.RetryBaseDelay)
	}
}

// TestConfigDefaultsNoOverwrite verifies explicit values are preserved.
func TestConfigDefaultsNoOverwrite(t *testing.T) {
	cfg := Config{
		Brokers:           []string{"localhost:9092"},
		Topic:             "test-topic",
		SessionTimeout:    10 * time.Minute,
		HeartbeatInterval: 1 * time.Second,
		MaxWait:           2 * time.Second,
		RetryAttempts:     3,
		RetryBaseDelay:    500 * time.Millisecond,
	}
	cfg.defaults()

	if cfg.SessionTimeout != 10*time.Minute {
		t.Errorf("SessionTimeout overwritten: got %v", cfg.SessionTimeout)
	}
	if cfg.HeartbeatInterval != 1*time.Second {
		t.Errorf("HeartbeatInterval overwritten: got %v", cfg.HeartbeatInterval)
	}
	if cfg.MaxWait != 2*time.Second {
		t.Errorf("MaxWait overwritten: got %v", cfg.MaxWait)
	}
	if cfg.RetryAttempts != 3 {
		t.Errorf("RetryAttempts overwritten: got %d", cfg.RetryAttempts)
	}
	if cfg.RetryBaseDelay != 500*time.Millisecond {
		t.Errorf("RetryBaseDelay overwritten: got %v", cfg.RetryBaseDelay)
	}
}

// TestMockPublishAndConsume verifies mock round-trip for interface consumers.
func TestMockPublishAndConsume(t *testing.T) {
	msg := pkg.KafkaMessage{
		JobID:       "test-job-123",
		SourceS3URI: "s3://bucket/key",
		Renditions:  []pkg.Rendition{{ID: "720p", Resolution: "1280x720"}},
		RetryCount:  0,
		MaxRetries:  3,
	}

	mq := &mockQueue{
		messages: []*Message{{Parsed: msg}},
	}

	// Publish
	if err := mq.Publish(context.Background(), msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if len(mq.published) != 1 {
		t.Fatalf("expected 1 published, got %d", len(mq.published))
	}
	if mq.published[0].JobID != "test-job-123" {
		t.Errorf("wrong job_id: %s", mq.published[0].JobID)
	}

	// Consume
	consumed, err := mq.Consume(context.Background())
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	if consumed.Parsed.JobID != "test-job-123" {
		t.Errorf("consumed wrong job_id: %s", consumed.Parsed.JobID)
	}

	// Commit
	if err := mq.Commit(context.Background(), consumed); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if len(mq.commitLog) != 1 {
		t.Errorf("expected 1 commit, got %d", len(mq.commitLog))
	}
}

// TestMockSendDLQ verifies DLQ mock captures reason.
func TestMockSendDLQ(t *testing.T) {
	msg := pkg.KafkaMessage{
		JobID:      "dead-job-456",
		RetryCount: 3,
	}

	mq := &mockQueue{}
	reason := "ffmpeg: unsupported codec"

	if err := mq.SendDLQ(context.Background(), msg, reason); err != nil {
		t.Fatalf("SendDLQ failed: %v", err)
	}
	if len(mq.dlqMessages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(mq.dlqMessages))
	}
	if mq.dlqMessages[0].JobID != "dead-job-456" {
		t.Errorf("wrong dlq job_id: %s", mq.dlqMessages[0].JobID)
	}
	if mq.dlqReasons[0] != reason {
		t.Errorf("wrong dlq reason: %s", mq.dlqReasons[0])
	}
}

// TestMessageAccessors verifies Offset() and Partition() helpers.
func TestMessageAccessors(t *testing.T) {
	// Cannot import kafka-go Message directly in test without broker,
	// but we can test the accessor methods work on a zero-value.
	msg := &Message{}
	if msg.Offset() != 0 {
		t.Errorf("expected offset 0, got %d", msg.Offset())
	}
	if msg.Partition() != 0 {
		t.Errorf("expected partition 0, got %d", msg.Partition())
	}
}
