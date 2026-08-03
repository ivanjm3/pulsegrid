package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// DLQTopic is the Kafka topic permanently-failed and retry-exhausted jobs are
// published to.
const DLQTopic = "transcoding-dlq"

// DLQMessage is the wire format published to the transcoding-dlq topic. It
// extends JobMessage with failure context, matching the design doc's DLQ
// Message Schema.
type DLQMessage struct {
	JobMessage
	DLQEntryTimestamp string `json:"dlq_entry_timestamp"`
	FailureReason     string `json:"failure_reason"`
	FailureTimestamp  string `json:"failure_timestamp"`
	PodID             string `json:"pod_id"`
	StderrSnippet     string `json:"stderr_snippet,omitempty"`
}

// NewKafkaDLQWriter returns a *kafka.Writer configured for the
// transcoding-dlq topic, partitioning by job_id hash like the main job
// writer.
func NewKafkaDLQWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    DLQTopic,
		Balancer: &kafka.Hash{},
	}
}

// DLQProducer publishes dead-letter messages to the transcoding-dlq topic.
type DLQProducer struct {
	writer Writer

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewDLQProducer returns a DLQProducer that publishes via writer.
func NewDLQProducer(writer Writer) *DLQProducer {
	return &DLQProducer{writer: writer, sleep: time.Sleep}
}

// SendDLQ serializes msg to JSON and publishes it to the transcoding-dlq
// topic, partitioned by job_id hash. Transient publish errors are retried
// with exponential backoff.
func (p *DLQProducer) SendDLQ(ctx context.Context, msg DLQMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("send dlq: marshal message: %w", err)
	}

	kmsg := kafka.Message{
		Key:   []byte(msg.JobID),
		Value: body,
	}

	if err := writeWithRetry(ctx, p.writer, p.sleep, kmsg); err != nil {
		return fmt.Errorf("send dlq: %w", err)
	}
	return nil
}

// Close closes the underlying writer.
func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
