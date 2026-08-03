package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// LifecycleTopic is the Kafka topic worker pods publish job lifecycle
// events to, for the analytics pipeline (task 35).
const LifecycleTopic = "job-lifecycle-events"

// LifecycleEventType is one of the four points in a job's life a
// JobLifecycleEvent can mark, per the design doc's lifecycle event schema.
type LifecycleEventType string

const (
	EventJobStarted         LifecycleEventType = "job_started"
	EventRenditionCompleted LifecycleEventType = "rendition_completed"
	EventJobCompleted       LifecycleEventType = "job_completed"
	EventJobFailed          LifecycleEventType = "job_failed"
)

// JobLifecycleEvent is the wire format published to the
// job-lifecycle-events topic. RenditionID is set only for
// rendition_completed events; ErrorClass and ErrorReason only for
// job_failed. Fields are pointers (not omitempty) so the JSON always carries
// the key, explicitly null when not applicable.
type JobLifecycleEvent struct {
	JobID       string             `json:"job_id"`
	EventType   LifecycleEventType `json:"event_type"`
	RenditionID *string            `json:"rendition_id"`
	ErrorClass  *string            `json:"error_class"`
	ErrorReason *string            `json:"error_reason"`
	PodID       string             `json:"pod_id"`
	Timestamp   string             `json:"timestamp"`
}

// NewKafkaLifecycleWriter returns a *kafka.Writer configured for topic,
// partitioning by job_id hash (same as the transcoding-jobs topic) so a
// given job's events are delivered in order.
func NewKafkaLifecycleWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
}

// NewKafkaLifecycleReader returns a *kafka.Reader for topic, joining
// groupID, used by the analytics consumer (task 36).
func NewKafkaLifecycleReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})
}

// LifecycleProducer publishes job lifecycle events to the
// job-lifecycle-events topic. Publish is retried with the shared exponential
// backoff schedule, but callers treat it as fire-and-forget: a lifecycle
// event publish failure must never block or fail job processing (task 35).
type LifecycleProducer struct {
	writer Writer

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewLifecycleProducer returns a LifecycleProducer that publishes via
// writer.
func NewLifecycleProducer(writer Writer) *LifecycleProducer {
	return &LifecycleProducer{writer: writer, sleep: time.Sleep}
}

// PublishEvent serializes event to JSON and publishes it to the
// job-lifecycle-events topic, partitioned by job_id hash.
func (p *LifecycleProducer) PublishEvent(ctx context.Context, event JobLifecycleEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("publish lifecycle event: marshal: %w", err)
	}

	kmsg := kafka.Message{
		Key:   []byte(event.JobID),
		Value: body,
	}

	if err := writeWithRetry(ctx, p.writer, p.sleep, kmsg); err != nil {
		return fmt.Errorf("publish lifecycle event: %w", err)
	}
	return nil
}

// Close closes the underlying writer.
func (p *LifecycleProducer) Close() error {
	return p.writer.Close()
}
