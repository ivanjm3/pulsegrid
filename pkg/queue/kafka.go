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

// maxPublishAttempts and the backoff schedule match the design doc's retry
// policy for Kafka publish: 500ms, 1s, 2s, 4s, 8s (max 5 attempts).
const maxPublishAttempts = 5

var backoffSchedule = []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

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

	var lastErr error
	for attempt := 0; attempt < maxPublishAttempts; attempt++ {
		if attempt > 0 {
			delay := backoffSchedule[attempt-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			p.sleep(delay)
		}

		if err := p.writer.WriteMessages(ctx, kmsg); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return fmt.Errorf("enqueue job: exhausted %d attempts: %w", maxPublishAttempts, lastErr)
}

// Close closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
