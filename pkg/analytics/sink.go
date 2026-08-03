package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"pulsegrid/pkg/queue"
)

// DB is the subset of *pgxpool.Pool used by PostgresSink, allowing tests to
// substitute a fake in-memory implementation (same pattern as
// pkg/store.DB).
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PostgresSink is the EventHandler that writes lifecycle events into
// analytics.job_lifecycle_events (task 37). Events are immutable facts, so
// SinkEvent only ever inserts — there is no upsert path.
type PostgresSink struct {
	db DB
}

// NewPostgresSink returns a PostgresSink backed by db.
func NewPostgresSink(db DB) *PostgresSink {
	return &PostgresSink{db: db}
}

// HandleEvent inserts event into analytics.job_lifecycle_events, satisfying
// the Consumer's EventHandler interface. received_at is left to the
// column's DEFAULT NOW() so it always reflects server time, never
// event.Timestamp.
func (s *PostgresSink) HandleEvent(ctx context.Context, event queue.JobLifecycleEvent) error {
	return s.SinkEvent(ctx, event)
}

// SinkEvent inserts a single lifecycle event. A failed insert is returned
// as an error so the caller (Consumer.processMessage) does not commit the
// Kafka offset — the event is redelivered on restart/rebalance.
func (s *PostgresSink) SinkEvent(ctx context.Context, event queue.JobLifecycleEvent) error {
	eventTime, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		return fmt.Errorf("sink lifecycle event: parse timestamp %q: %w", event.Timestamp, err)
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO analytics.job_lifecycle_events (
			job_id, event_type, rendition_id, error_class, error_reason, pod_id, event_time
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`,
		event.JobID, string(event.EventType), event.RenditionID,
		event.ErrorClass, event.ErrorReason, event.PodID, eventTime.UTC(),
	)
	if err != nil {
		return fmt.Errorf("sink lifecycle event: %w", err)
	}
	return nil
}
