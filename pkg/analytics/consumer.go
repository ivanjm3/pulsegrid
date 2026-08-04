// Package analytics implements the analytics consumer: a standalone service
// that consumes job lifecycle events published by worker pods (task 35) and
// sinks them for the analytics pipeline (task 36).
package analytics

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg/queue"
)

// GroupID is the default Kafka consumer group the analytics consumer joins.
const GroupID = "pulsegrid-analytics"

// EventHandler sinks a single decoded lifecycle event. A returned error
// prevents the offset from being committed, so the event is redelivered
// after a rebalance or restart — the at-least-once delivery gate for
// Requirement 19.4.
type EventHandler interface {
	HandleEvent(ctx context.Context, event queue.JobLifecycleEvent) error
}

// Reader is the subset of *kafka.Reader used by Consumer, allowing tests to
// substitute a fake.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Metrics records the analytics consumer's Prometheus metrics (task 40).
// Satisfied by *pkg/metrics.AnalyticsMetrics.
type Metrics interface {
	IncEventsProcessed(eventType string)
	SetSinkLag(seconds float64)
	IncConsumerError(errorType string)
}

// noopMetrics discards every call, used when NewConsumer is given a nil
// Metrics so Consumer never needs to nil-check before recording.
type noopMetrics struct{}

func (noopMetrics) IncEventsProcessed(string) {}
func (noopMetrics) SetSinkLag(float64)        {}
func (noopMetrics) IncConsumerError(string)   {}

// Consumer runs the analytics consumer's poll-process-commit loop against
// the job-lifecycle-events topic. Structurally identical to the worker
// pod's consumer loop (task 12): the offset only advances after the
// handler's sink write succeeds, never on a best-effort basis.
type Consumer struct {
	reader  Reader
	handler EventHandler
	metrics Metrics
}

// NewConsumer returns a Consumer that reads from reader and dispatches
// events to handler, recording Prometheus metrics via metrics (nil is
// accepted and treated as a no-op recorder).
func NewConsumer(reader Reader, handler EventHandler, metrics Metrics) *Consumer {
	if metrics == nil {
		metrics = noopMetrics{}
	}
	return &Consumer{reader: reader, handler: handler, metrics: metrics}
}

// Run polls for lifecycle events and processes them until ctx is cancelled.
//
// ctx cancellation (e.g. on SIGTERM) stops the loop from starting new work.
// An event already fetched is always processed to completion with an
// independent context before the loop checks ctx again, so an in-flight
// sink write is never aborted mid-way — it finishes, commits (or not), and
// only then does the loop exit. Run always closes the reader before
// returning, including on error.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return c.reader.Close()
			}
			log.Printf("event=kafka_poll_error error=%v", err)
			c.metrics.IncConsumerError("kafka_poll_error")
			continue
		}

		c.processMessage(context.Background(), msg)
	}
}

// processMessage decodes and sinks a single fetched lifecycle event,
// committing its offset only on a successful sink write.
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	var event queue.JobLifecycleEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("event=lifecycle_event_parse_error error=%v partition=%d offset=%d", err, msg.Partition, msg.Offset)
		c.metrics.IncConsumerError("parse_error")
		return
	}

	if err := c.handler.HandleEvent(ctx, event); err != nil {
		log.Printf("event=sink_write_failed job_id=%s error=%v", event.JobID, err)
		c.metrics.IncConsumerError("sink_write_failure")
		return
	}

	c.metrics.IncEventsProcessed(string(event.EventType))
	if eventTime, err := time.Parse(time.RFC3339, event.Timestamp); err == nil {
		c.metrics.SetSinkLag(time.Since(eventTime).Seconds())
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("event=commit_offset_failed job_id=%s error=%v", event.JobID, err)
	}
}
