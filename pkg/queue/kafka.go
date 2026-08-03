// Package queue implements the Kafka job queue producer used by the API
// server to enqueue transcoding jobs.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg"
)

// Topic is the Kafka topic transcoding jobs are published to.
const Topic = "transcoding-jobs"

// DefaultMaxRetries and DefaultVisibilityTimeoutSeconds match the job queue
// contract in the design doc.
const (
	DefaultMaxRetries               = 3
	DefaultVisibilityTimeoutSeconds = 1800
)

// maxPublishAttempts and publishBaseDelay match the design doc's retry
// policy for Kafka publish: 500ms, 1s, 2s, 4s, 8s (max 5 attempts).
const (
	maxPublishAttempts = 5
	publishBaseDelay   = 500 * time.Millisecond
)

// JobMessage is the wire format published to the transcoding-jobs topic.
// Field names and shape follow the design doc's Job Message Schema.
type JobMessage struct {
	JobID                    string          `json:"job_id"`
	SourceS3URI              string          `json:"source_s3_uri"`
	Renditions               []pkg.Rendition `json:"renditions"`
	OutputS3Prefix           string          `json:"output_s3_prefix"`
	RetryCount               int             `json:"retry_count"`
	MaxRetries               int             `json:"max_retries"`
	SubmittedTimestamp       string          `json:"submitted_timestamp"`
	VisibilityTimeoutSeconds int             `json:"visibility_timeout_seconds"`
}

// NewJobMessage builds the Kafka job message for job, filling in queue
// defaults for max_retries and visibility_timeout_seconds.
func NewJobMessage(job pkg.Job) JobMessage {
	renditions := job.Renditions
	if renditions == nil {
		renditions = []pkg.Rendition{}
	}
	return JobMessage{
		JobID:                    job.ID,
		SourceS3URI:              job.SourceS3URI,
		Renditions:               renditions,
		OutputS3Prefix:           job.OutputS3Prefix,
		RetryCount:               job.RetryCount,
		MaxRetries:               DefaultMaxRetries,
		SubmittedTimestamp:       job.SubmissionTime.UTC().Format(time.RFC3339),
		VisibilityTimeoutSeconds: DefaultVisibilityTimeoutSeconds,
	}
}

// Writer is the subset of *kafka.Writer used by Producer, allowing tests to
// substitute a fake.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer publishes job messages to the transcoding-jobs topic.
type Producer struct {
	writer Writer

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewProducer returns a Producer that publishes via writer.
func NewProducer(writer Writer) *Producer {
	return &Producer{writer: writer, sleep: time.Sleep}
}

// NewKafkaWriter returns a *kafka.Writer configured for the transcoding-jobs
// topic, partitioning by job_id hash (kafka.Hash balances on message Key).
func NewKafkaWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    Topic,
		Balancer: &kafka.Hash{},
	}
}

// EnqueueJob serializes job to JSON and publishes it to the transcoding-jobs
// topic, partitioned by job_id hash. Transient publish errors are retried
// with exponential backoff.
func (p *Producer) EnqueueJob(ctx context.Context, job pkg.Job) error {
	msg := NewJobMessage(job)

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("enqueue job: marshal message: %w", err)
	}

	kmsg := kafka.Message{
		Key:   []byte(job.ID),
		Value: body,
	}

	if err := writeWithRetry(ctx, p.writer, p.sleep, kmsg); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}
	return nil
}

// writeWithRetry publishes kmsg via writer, retrying transient failures with
// the shared exponential backoff schedule (500ms, 1s, 2s, 4s, 8s; max 5
// attempts). Shared by Producer.EnqueueJob and DLQProducer.SendDLQ.
func writeWithRetry(ctx context.Context, writer Writer, sleep func(time.Duration), kmsg kafka.Message) error {
	return pkg.RetryWithBackoff(ctx, maxPublishAttempts, publishBaseDelay, sleep, func(ctx context.Context) error {
		return writer.WriteMessages(ctx, kmsg)
	})
}

// Close closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// MessageQueue is the job queue contract shared by the API server (Publish)
// and worker pod (Consume, Commit, SendDLQ). It exists so callers can depend
// on one small interface instead of wiring Producer, DLQProducer, and a raw
// *kafka.Reader separately, and so tests can substitute a single in-memory
// fake for the whole queue. KafkaQueue is the Kafka-backed implementation.
type MessageQueue interface {
	// Publish enqueues job to the transcoding-jobs topic.
	Publish(ctx context.Context, job pkg.Job) error
	// Consume fetches the next transcoding-jobs message for this consumer's
	// assigned partitions, blocking until one is available or ctx is
	// cancelled. The returned ConsumedMessage must be passed to Commit after
	// it has been fully processed.
	Consume(ctx context.Context) (ConsumedMessage, error)
	// Commit marks msg as processed, advancing the consumer group's offset.
	Commit(ctx context.Context, msg ConsumedMessage) error
	// SendDLQ publishes msg to the transcoding-dlq topic.
	SendDLQ(ctx context.Context, msg DLQMessage) error
}

// ConsumedMessage pairs a decoded JobMessage with the underlying Kafka
// message needed to commit its offset. Only KafkaQueue.Consume constructs
// one; callers pass it straight through to Commit.
type ConsumedMessage struct {
	Job JobMessage

	raw kafka.Message
}

// Reader is the subset of *kafka.Reader used by KafkaQueue.Consume/Commit,
// allowing tests to substitute a fake.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaQueue implements MessageQueue on top of Kafka, delegating to a
// Producer (transcoding-jobs), a Reader (consumer group), and a DLQProducer
// (transcoding-dlq). It adds no logic of its own beyond decode-on-consume —
// retry/backoff, partitioning, and consumer group semantics all live in the
// composed pieces.
type KafkaQueue struct {
	producer *Producer
	reader   Reader
	dlq      *DLQProducer
}

// NewKafkaQueue returns a MessageQueue backed by producer (publish), reader
// (consume/commit), and dlq (dead-letter publish).
func NewKafkaQueue(producer *Producer, reader Reader, dlq *DLQProducer) *KafkaQueue {
	return &KafkaQueue{producer: producer, reader: reader, dlq: dlq}
}

// Publish enqueues job via the underlying Producer.
func (q *KafkaQueue) Publish(ctx context.Context, job pkg.Job) error {
	return q.producer.EnqueueJob(ctx, job)
}

// Consume fetches and decodes the next transcoding-jobs message.
func (q *KafkaQueue) Consume(ctx context.Context) (ConsumedMessage, error) {
	msg, err := q.reader.FetchMessage(ctx)
	if err != nil {
		return ConsumedMessage{}, err
	}

	var jobMsg JobMessage
	if err := json.Unmarshal(msg.Value, &jobMsg); err != nil {
		return ConsumedMessage{}, fmt.Errorf("consume: decode message: %w", err)
	}

	return ConsumedMessage{Job: jobMsg, raw: msg}, nil
}

// Commit advances the consumer group's offset past msg.
func (q *KafkaQueue) Commit(ctx context.Context, msg ConsumedMessage) error {
	return q.reader.CommitMessages(ctx, msg.raw)
}

// SendDLQ publishes msg via the underlying DLQProducer.
func (q *KafkaQueue) SendDLQ(ctx context.Context, msg DLQMessage) error {
	return q.dlq.SendDLQ(ctx, msg)
}

// Close closes the producer, reader, and DLQ producer.
func (q *KafkaQueue) Close() error {
	err := q.producer.Close()
	if cerr := q.reader.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if cerr := q.dlq.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// Pinger checks broker reachability for health checks.
type Pinger struct {
	Brokers []string
}

// Ping dials the first configured broker and closes the connection
// immediately. A successful dial confirms the broker is reachable.
func (p *Pinger) Ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", p.Brokers[0])
	if err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return conn.Close()
}

// QueueDepth queries the Kafka admin API for the transcoding-jobs topic's
// partitions and returns the sum of their log-end offsets (high watermarks).
// This approximates queue depth as total unconsumed messages; it is not true
// consumer-group lag since no consumer group exists yet (see task 9.2 —
// deferred until the worker consumer, task 12, is in place).
func QueueDepth(ctx context.Context, brokers []string) (int64, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return 0, fmt.Errorf("queue depth: dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(Topic)
	if err != nil {
		return 0, fmt.Errorf("queue depth: read partitions: %w", err)
	}

	var total int64
	for _, p := range partitions {
		pconn, err := kafka.DialLeader(ctx, "tcp", p.Leader.Host+fmt.Sprintf(":%d", p.Leader.Port), Topic, p.ID)
		if err != nil {
			return 0, fmt.Errorf("queue depth: dial partition %d leader: %w", p.ID, err)
		}
		lastOffset, err := pconn.ReadLastOffset()
		pconn.Close()
		if err != nil {
			return 0, fmt.Errorf("queue depth: read last offset partition %d: %w", p.ID, err)
		}
		total += lastOffset
	}

	return total, nil
}
