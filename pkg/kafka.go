package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer is the interface for publishing jobs to Kafka.
// Allows mocking in tests and nil-check for local dev.
type KafkaProducer interface {
	EnqueueJob(ctx context.Context, job Job) error
	SendDLQ(ctx context.Context, job Job, reason string) error
	Close() error
}

// KafkaMessage is the JSON schema published to Kafka transcoding-jobs topic.
type KafkaMessage struct {
	JobID                  string      `json:"job_id"`
	SourceS3URI            string      `json:"source_s3_uri"`
	SourceFileSizeBytes    int64       `json:"source_file_size_bytes"`
	Renditions             []Rendition `json:"renditions"`
	OutputS3Prefix         string      `json:"output_s3_prefix"`
	RetryCount             int         `json:"retry_count"`
	MaxRetries             int         `json:"max_retries"`
	SubmittedTimestamp     string      `json:"submitted_timestamp"`
	VisibilityTimeoutSecs  int         `json:"visibility_timeout_seconds"`
}

// DLQMessage extends KafkaMessage with failure metadata.
type DLQMessage struct {
	KafkaMessage
	DLQEntryTimestamp string `json:"dlq_entry_timestamp"`
	FailureReason     string `json:"failure_reason"`
	FailureTimestamp  string `json:"failure_timestamp"`
}

// KafkaClient implements KafkaProducer using segmentio/kafka-go.
type KafkaClient struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	topic     string
	dlqTopic  string
}

// NewKafkaClient creates KafkaClient with writers for main topic and DLQ.
func NewKafkaClient(brokers []string, topic string, dlqTopic string) *KafkaClient {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1, // We handle retries ourselves via RetryWithBackoff
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
	}

	return &KafkaClient{
		writer:    writer,
		dlqWriter: dlqWriter,
		topic:     topic,
		dlqTopic:  dlqTopic,
	}
}

// EnqueueJob serializes Job to JSON and publishes to Kafka.
// Partitions by job_id hash. Retries with exponential backoff (1s base, 16s cap, 5 attempts).
func (c *KafkaClient) EnqueueJob(ctx context.Context, job Job) error {
	msg := KafkaMessage{
		JobID:                  job.JobID,
		SourceS3URI:            job.SourceS3URI,
		SourceFileSizeBytes:    job.SourceFileSizeBytes,
		Renditions:             job.Renditions,
		OutputS3Prefix:         job.OutputS3Prefix,
		RetryCount:             job.RetryCount,
		MaxRetries:             job.MaxRetries,
		SubmittedTimestamp:     job.SubmissionTime.UTC().Format(time.RFC3339Nano),
		VisibilityTimeoutSecs:  job.VisibilityTimeoutSecs,
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka marshal failed for job %s: %w", job.JobID, err)
	}

	err = RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		return c.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(job.JobID),
			Value: value,
		})
	})

	if err != nil {
		return fmt.Errorf("kafka enqueue failed for job %s: %w", job.JobID, err)
	}

	return nil
}

// SendDLQ publishes failed job to dead-letter queue topic.
// Retries with same backoff as EnqueueJob.
func (c *KafkaClient) SendDLQ(ctx context.Context, job Job, reason string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	dlqMsg := DLQMessage{
		KafkaMessage: KafkaMessage{
			JobID:                  job.JobID,
			SourceS3URI:            job.SourceS3URI,
			SourceFileSizeBytes:    job.SourceFileSizeBytes,
			Renditions:             job.Renditions,
			OutputS3Prefix:         job.OutputS3Prefix,
			RetryCount:             job.RetryCount,
			MaxRetries:             job.MaxRetries,
			SubmittedTimestamp:     job.SubmissionTime.UTC().Format(time.RFC3339Nano),
			VisibilityTimeoutSecs:  job.VisibilityTimeoutSecs,
		},
		DLQEntryTimestamp: now,
		FailureReason:     reason,
		FailureTimestamp:  now,
	}

	value, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("kafka dlq marshal failed for job %s: %w", job.JobID, err)
	}

	err = RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		return c.dlqWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(job.JobID),
			Value: value,
		})
	})

	if err != nil {
		return fmt.Errorf("kafka dlq send failed for job %s: %w", job.JobID, err)
	}

	return nil
}

// Close closes both Kafka writers.
func (c *KafkaClient) Close() error {
	var errs []error
	if err := c.writer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close writer: %w", err))
	}
	if err := c.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dlq writer: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("kafka close errors: %v", errs)
	}
	return nil
}

// HashPartition returns partition number for a job_id using FNV-1a hash.
// Used for documentation/testing — kafka-go's Hash balancer does this internally.
func HashPartition(jobID string, partitionCount int) int {
	h := fnv.New32a()
	h.Write([]byte(jobID))
	return int(h.Sum32()) % partitionCount
}
