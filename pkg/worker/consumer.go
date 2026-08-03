// Package worker implements the worker pod's Kafka job consumer loop and
// source download staging.
package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg/queue"
)

// GroupID is the Kafka consumer group all worker pods join.
const GroupID = "pulsegrid-workers"

// SessionTimeout is set well above the longest expected transcode (30 min
// default job timeout, see task 14) so a slow job never causes the broker to
// mistake a live worker for a dead one and trigger a rebalance mid-job.
const SessionTimeout = 30 * time.Minute

// MessageReader is the subset of *kafka.Reader used by Consumer, allowing
// tests to substitute a fake.
//
// Kafka consumer-group semantics (not SQS): there is no per-message lock or
// visibility timeout. FetchMessage returns the next message for a partition
// assigned to this consumer; CommitMessages marks it processed by advancing
// the committed offset. If the process crashes after FetchMessage but before
// CommitMessages, no offset advance happened — the broker detects the dead
// consumer via session timeout, rebalances the partition to another group
// member, and that member re-reads the same offset.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// JobHandler processes a single job decoded from a transcoding-jobs message.
// A returned error prevents the offset from being committed, so the message
// is redelivered after a rebalance.
type JobHandler interface {
	HandleJob(ctx context.Context, msg queue.JobMessage) error
}

// Consumer runs the worker pod's poll-process-commit loop against the
// transcoding-jobs topic.
type Consumer struct {
	reader  MessageReader
	handler JobHandler
}

// NewConsumer returns a Consumer that reads from reader and dispatches jobs
// to handler.
func NewConsumer(reader MessageReader, handler JobHandler) *Consumer {
	return &Consumer{reader: reader, handler: handler}
}

// Run polls for messages and processes them until ctx is cancelled.
//
// ctx cancellation (e.g. on SIGTERM) stops the loop from starting new work:
// it unblocks a pending FetchMessage and prevents further polling. A job
// already fetched is always processed to completion with an independent
// context before the loop checks ctx again, so an in-flight transcode is
// never aborted mid-way — it finishes, commits (or not), and only then does
// the loop exit. Run always closes the reader before returning, including on
// error.
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
			continue
		}

		c.processMessage(context.Background(), msg)
	}
}

// processMessage decodes and handles a single fetched message, committing
// its offset only on successful processing.
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	var jobMsg queue.JobMessage
	if err := json.Unmarshal(msg.Value, &jobMsg); err != nil {
		log.Printf("event=job_message_parse_error error=%v partition=%d offset=%d", err, msg.Partition, msg.Offset)
		return
	}

	if err := c.handler.HandleJob(ctx, jobMsg); err != nil {
		log.Printf("event=job_processing_failed job_id=%s error=%v", jobMsg.JobID, err)
		return
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("event=commit_offset_failed job_id=%s error=%v", jobMsg.JobID, err)
	}
}

// NewKafkaReader returns a *kafka.Reader configured for the transcoding-jobs
// consumer group, with earliest auto offset reset (kafka.FirstOffset) and a
// session timeout long enough to tolerate slow transcodes.
func NewKafkaReader(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          queue.Topic,
		GroupID:        GroupID,
		StartOffset:    kafka.FirstOffset,
		SessionTimeout: SessionTimeout,
	})
}
