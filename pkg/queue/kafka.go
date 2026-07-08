// Package queue provides a unified Kafka producer+consumer abstraction
// for the Pulsegrid job queue. The MessageQueue interface encapsulates
// publish, consume, commit, and dead-letter operations with built-in
// retry logic, partition assignment, and consumer group management.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"pulsegrid/pkg"
)

// MessageQueue is the unified interface for producing and consuming
// job messages on Kafka. Designed for easy mocking in tests.
type MessageQueue interface {
	// Publish serializes and publishes a KafkaMessage to the main topic.
	// Partitions by job_id hash. Retries with exponential backoff.
	Publish(ctx context.Context, msg pkg.KafkaMessage) error

	// Consume fetches the next message from the consumer group.
	// Blocks until a message is available or context is cancelled.
	Consume(ctx context.Context) (*Message, error)

	// Commit acknowledges processing of a consumed message, advancing
	// the consumer group offset.
	Commit(ctx context.Context, msg *Message) error

	// SendDLQ publishes a failed message to the dead-letter queue topic
	// with failure metadata. Retries with exponential backoff.
	SendDLQ(ctx context.Context, msg pkg.KafkaMessage, reason string) error

	// Close shuts down the producer writers and consumer reader.
	Close() error
}

// Message wraps a raw kafka.Message with the deserialized KafkaMessage payload.
type Message struct {
	// Parsed is the deserialized job message from the Kafka value bytes.
	Parsed pkg.KafkaMessage

	// raw is the underlying kafka-go message (needed for commit).
	raw kafka.Message
}

// Offset returns the Kafka offset of this message.
func (m *Message) Offset() int64 { return m.raw.Offset }

// Partition returns the Kafka partition of this message.
func (m *Message) Partition() int { return m.raw.Partition }

// Config holds all configuration for creating a KafkaQueue.
type Config struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// Topic is the main job queue topic (e.g. "transcoding-jobs").
	Topic string

	// DLQTopic is the dead-letter queue topic (e.g. "transcoding-dlq").
	DLQTopic string

	// GroupID is the consumer group identifier (e.g. "pulsegrid-workers").
	GroupID string

	// SessionTimeout is the consumer group session timeout.
	// Set high (30min) for long-running transcodes to prevent rebalance.
	// Default: 30 minutes.
	SessionTimeout time.Duration

	// HeartbeatInterval is how often heartbeats are sent to the broker.
	// Default: 3 seconds.
	HeartbeatInterval time.Duration

	// MaxWait is the maximum time to wait for new messages during fetch.
	// Default: 5 seconds.
	MaxWait time.Duration

	// RetryAttempts is the number of publish retry attempts.
	// Default: 5.
	RetryAttempts int

	// RetryBaseDelay is the base delay for exponential backoff on publish.
	// Default: 1 second.
	RetryBaseDelay time.Duration

	// PodID identifies the worker pod (included in DLQ messages).
	PodID string
}

// defaults fills zero-value fields with sensible production defaults.
func (c *Config) defaults() {
	if c.SessionTimeout == 0 {
		c.SessionTimeout = 30 * time.Minute
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 3 * time.Second
	}
	if c.MaxWait == 0 {
		c.MaxWait = 5 * time.Second
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = 5
	}
	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = 1 * time.Second
	}
}

// KafkaQueue implements MessageQueue using segmentio/kafka-go.
type KafkaQueue struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	reader    *kafka.Reader
	cfg       Config
}

// NewKafkaQueue creates a fully-configured KafkaQueue with producer writers
// and consumer reader. The consumer uses a consumer group with a long session
// timeout (30min default) to prevent rebalance during long transcodes.
func NewKafkaQueue(cfg Config) *KafkaQueue {
	cfg.defaults()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1, // Retries handled by RetryWithBackoff
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		Topic:             cfg.Topic,
		GroupID:           cfg.GroupID,
		StartOffset:       kafka.FirstOffset,
		SessionTimeout:    cfg.SessionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		MaxWait:           cfg.MaxWait,
	})

	return &KafkaQueue{
		writer:    writer,
		dlqWriter: dlqWriter,
		reader:    reader,
		cfg:       cfg,
	}
}

// Publish serializes msg to JSON and publishes to the main topic.
// Uses job_id as partition key (Hash balancer). Retries with exponential backoff.
func (q *KafkaQueue) Publish(ctx context.Context, msg pkg.KafkaMessage) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue publish marshal failed for job %s: %w", msg.JobID, err)
	}

	err = pkg.RetryWithBackoff(ctx, q.cfg.RetryAttempts, q.cfg.RetryBaseDelay, func() error {
		return q.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(msg.JobID),
			Value: value,
		})
	})
	if err != nil {
		return fmt.Errorf("queue publish failed for job %s: %w", msg.JobID, err)
	}

	return nil
}

// Consume fetches the next message from the consumer group and deserializes it.
// Blocks until a message is available or the context is cancelled/timed out.
func (q *KafkaQueue) Consume(ctx context.Context) (*Message, error) {
	raw, err := q.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue consume failed: %w", err)
	}

	var parsed pkg.KafkaMessage
	if err := json.Unmarshal(raw.Value, &parsed); err != nil {
		// Return message with zero-value Parsed so caller can still commit (skip malformed)
		return &Message{raw: raw}, fmt.Errorf("queue consume unmarshal failed at offset %d: %w", raw.Offset, err)
	}

	return &Message{
		Parsed: parsed,
		raw:    raw,
	}, nil
}

// Commit commits the offset of a consumed message, advancing the consumer group.
func (q *KafkaQueue) Commit(ctx context.Context, msg *Message) error {
	if err := q.reader.CommitMessages(ctx, msg.raw); err != nil {
		return fmt.Errorf("queue commit failed at offset %d: %w", msg.raw.Offset, err)
	}
	return nil
}

// SendDLQ publishes the message to the dead-letter queue with failure metadata.
// Retries with exponential backoff.
func (q *KafkaQueue) SendDLQ(ctx context.Context, msg pkg.KafkaMessage, reason string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	dlqMsg := pkg.DLQMessage{
		KafkaMessage:      msg,
		DLQEntryTimestamp: now,
		FailureReason:     reason,
		FailureTimestamp:  now,
		PodID:             q.cfg.PodID,
	}

	value, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("queue dlq marshal failed for job %s: %w", msg.JobID, err)
	}

	err = pkg.RetryWithBackoff(ctx, q.cfg.RetryAttempts, q.cfg.RetryBaseDelay, func() error {
		return q.dlqWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(msg.JobID),
			Value: value,
		})
	})
	if err != nil {
		return fmt.Errorf("queue dlq send failed for job %s: %w", msg.JobID, err)
	}

	return nil
}

// Close shuts down the Kafka writers and consumer reader.
func (q *KafkaQueue) Close() error {
	var errs []error
	if err := q.writer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close writer: %w", err))
	}
	if err := q.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dlq writer: %w", err))
	}
	if err := q.reader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close reader: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("queue close errors: %v", errs)
	}
	return nil
}
