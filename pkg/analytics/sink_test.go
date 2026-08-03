package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"pulsegrid/pkg/queue"
)

// fakeDB records every Exec call so tests can assert on the SQL and
// arguments a PostgresSink actually sends, without a real database.
type fakeDB struct {
	sql  string
	args []any
	err  error
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.sql = sql
	f.args = args
	return pgconn.CommandTag{}, f.err
}

func strPtr(s string) *string { return &s }

func TestPostgresSink_SinkEvent_Success(t *testing.T) {
	db := &fakeDB{}
	sink := NewPostgresSink(db)

	event := queue.JobLifecycleEvent{
		JobID:       "job-1",
		EventType:   queue.EventRenditionCompleted,
		RenditionID: strPtr("720p"),
		ErrorClass:  nil,
		ErrorReason: nil,
		PodID:       "worker-abc",
		Timestamp:   "2026-08-03T10:00:00Z",
	}

	if err := sink.SinkEvent(context.Background(), event); err != nil {
		t.Fatalf("SinkEvent: unexpected error: %v", err)
	}

	if !strings.Contains(db.sql, "INSERT INTO analytics.job_lifecycle_events") {
		t.Errorf("sql = %q, want INSERT INTO analytics.job_lifecycle_events", db.sql)
	}
	if len(db.args) != 7 {
		t.Fatalf("args count = %d, want 7", len(db.args))
	}
	if db.args[0] != event.JobID {
		t.Errorf("args[0] (job_id) = %v, want %v", db.args[0], event.JobID)
	}
	if db.args[1] != string(event.EventType) {
		t.Errorf("args[1] (event_type) = %v, want %v", db.args[1], event.EventType)
	}
	gotEventTime, ok := db.args[6].(time.Time)
	if !ok {
		t.Fatalf("args[6] (event_time) type = %T, want time.Time", db.args[6])
	}
	wantEventTime, _ := time.Parse(time.RFC3339, event.Timestamp)
	if !gotEventTime.Equal(wantEventTime) {
		t.Errorf("event_time = %v, want %v", gotEventTime, wantEventTime)
	}
}

func TestPostgresSink_SinkEvent_InsertFails_ReturnsError(t *testing.T) {
	db := &fakeDB{err: errors.New("connection reset")}
	sink := NewPostgresSink(db)

	event := queue.JobLifecycleEvent{
		JobID:     "job-2",
		EventType: queue.EventJobStarted,
		PodID:     "worker-abc",
		Timestamp: "2026-08-03T10:00:00Z",
	}

	err := sink.SinkEvent(context.Background(), event)
	if err == nil {
		t.Fatal("SinkEvent: expected error, got nil")
	}
}

func TestPostgresSink_HandleEvent_InsertFails_ReturnsError(t *testing.T) {
	// The Consumer gates its Kafka offset commit on EventHandler.HandleEvent
	// returning an error, so HandleEvent must surface a failed insert the
	// same way SinkEvent does.
	db := &fakeDB{err: errors.New("write failed")}
	sink := NewPostgresSink(db)

	event := queue.JobLifecycleEvent{
		JobID:     "job-3",
		EventType: queue.EventJobFailed,
		PodID:     "worker-abc",
		Timestamp: "2026-08-03T10:00:00Z",
	}

	if err := sink.HandleEvent(context.Background(), event); err == nil {
		t.Fatal("HandleEvent: expected error, got nil")
	}
}

func TestPostgresSink_SinkEvent_OnlyTouchesAnalyticsSchema(t *testing.T) {
	db := &fakeDB{}
	sink := NewPostgresSink(db)

	event := queue.JobLifecycleEvent{
		JobID:     "job-4",
		EventType: queue.EventJobCompleted,
		PodID:     "worker-abc",
		Timestamp: "2026-08-03T10:00:00Z",
	}

	if err := sink.SinkEvent(context.Background(), event); err != nil {
		t.Fatalf("SinkEvent: unexpected error: %v", err)
	}

	if !strings.Contains(db.sql, "analytics.job_lifecycle_events") {
		t.Errorf("sql = %q, expected to target analytics.job_lifecycle_events", db.sql)
	}
	if strings.Contains(db.sql, "INTO jobs") || strings.Contains(db.sql, "INTO job_status_events") {
		t.Errorf("sql = %q, must not touch public schema tables", db.sql)
	}
}

func TestPostgresSink_SinkEvent_ReceivedAtNotSetFromEventTime(t *testing.T) {
	// received_at has a DEFAULT NOW() on the column (migration 003) and is
	// deliberately never passed as a parameter, so the server's clock, not
	// event.Timestamp, always determines it. Verify the INSERT's column
	// list omits received_at and only 7 args (not 8) are bound.
	db := &fakeDB{}
	sink := NewPostgresSink(db)

	event := queue.JobLifecycleEvent{
		JobID:     "job-5",
		EventType: queue.EventJobStarted,
		PodID:     "worker-abc",
		Timestamp: "2026-08-03T10:00:00Z",
	}

	if err := sink.SinkEvent(context.Background(), event); err != nil {
		t.Fatalf("SinkEvent: unexpected error: %v", err)
	}

	if strings.Contains(db.sql, "received_at") {
		t.Errorf("sql = %q, must not set received_at explicitly", db.sql)
	}
	if len(db.args) != 7 {
		t.Errorf("args count = %d, want 7 (no received_at param)", len(db.args))
	}
}
