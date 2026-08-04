package analytics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeQueryDB records the last SQL issued and replays a fixed set of rows
// (as generic []any values), so tests can assert both the query text
// (schema isolation) and the scanning logic, without a real database.
type fakeQueryDB struct {
	lastSQL string
	rowVals [][]any
	err     error
}

func (f *fakeQueryDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.lastSQL = sql
	if f.err != nil {
		return nil, f.err
	}
	return &fakeQueryRows{rows: f.rowVals}, nil
}

type fakeQueryRows struct {
	rows [][]any
	i    int
}

func (r *fakeQueryRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeQueryRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	for i, d := range dest {
		switch p := d.(type) {
		case *time.Time:
			*p = row[i].(time.Time)
		case *int:
			*p = row[i].(int)
		case *string:
			*p = row[i].(string)
		case **string:
			*p = row[i].(*string)
		case *float64:
			*p = row[i].(float64)
		case **float64:
			*p = row[i].(*float64)
		default:
			return fmt.Errorf("fakeQueryRows.Scan: unsupported dest type %T", d)
		}
	}
	return nil
}

func (r *fakeQueryRows) Err() error                                   { return nil }
func (r *fakeQueryRows) Close()                                       {}
func (r *fakeQueryRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeQueryRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeQueryRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeQueryRows) RawValues() [][]byte                          { return nil }
func (r *fakeQueryRows) Conn() *pgx.Conn                              { return nil }

func floatPtr(f float64) *float64 { return &f }

func TestFetchThroughputPerMinute(t *testing.T) {
	minute := time.Date(2026, 8, 3, 10, 30, 0, 0, time.UTC)
	db := &fakeQueryDB{rowVals: [][]any{{minute, 5}}}
	q := NewQueries(db)

	got, err := q.FetchThroughputPerMinute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || !got[0].Minute.Equal(minute) || got[0].JobsCompleted != 5 {
		t.Errorf("got %+v, want [{%v 5}]", got, minute)
	}
	if !strings.Contains(db.lastSQL, "analytics.v_throughput_per_minute") {
		t.Errorf("sql = %q, want to target analytics.v_throughput_per_minute", db.lastSQL)
	}
}

func TestFetchLatencyPercentiles(t *testing.T) {
	hour := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	db := &fakeQueryDB{rowVals: [][]any{{hour, floatPtr(10), floatPtr(20), floatPtr(30)}}}
	q := NewQueries(db)

	got, err := q.FetchLatencyPercentiles(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || *got[0].P50 != 10 || *got[0].P95 != 20 || *got[0].P99 != 30 {
		t.Errorf("got %+v", got)
	}
	if !strings.Contains(db.lastSQL, "analytics.v_latency_percentiles") {
		t.Errorf("sql = %q, want to target analytics.v_latency_percentiles", db.lastSQL)
	}
}

func TestFetchFailureRateByClass(t *testing.T) {
	hour := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	db := &fakeQueryDB{rowVals: [][]any{{hour, strPtr("permanent"), 3, floatPtr(75.0)}}}
	q := NewQueries(db)

	got, err := q.FetchFailureRateByClass(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || *got[0].ErrorClass != "permanent" || got[0].FailureCount != 3 || *got[0].FailureRatePct != 75.0 {
		t.Errorf("got %+v", got)
	}
	if !strings.Contains(db.lastSQL, "analytics.v_failure_rate_by_class") {
		t.Errorf("sql = %q, want to target analytics.v_failure_rate_by_class", db.lastSQL)
	}
}

func TestFetchRenditionBreakdown(t *testing.T) {
	db := &fakeQueryDB{rowVals: [][]any{{strPtr("720p"), 5, 2, floatPtr(42.5)}}}
	q := NewQueries(db)

	got, err := q.FetchRenditionBreakdown(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || *got[0].RenditionID != "720p" || got[0].CompletedCount != 5 || got[0].FailedCount != 2 || *got[0].AvgDurationSeconds != 42.5 {
		t.Errorf("got %+v", got)
	}
	if !strings.Contains(db.lastSQL, "analytics.v_rendition_breakdown") {
		t.Errorf("sql = %q, want to target analytics.v_rendition_breakdown", db.lastSQL)
	}
}

func TestQueries_NeverTouchPublicSchemaTables(t *testing.T) {
	minute := time.Now().UTC()
	db := &fakeQueryDB{rowVals: [][]any{{minute, 1}}}
	q := NewQueries(db)

	if _, err := q.FetchThroughputPerMinute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(db.lastSQL, "FROM jobs") || strings.Contains(db.lastSQL, "job_status_events") {
		t.Errorf("sql = %q, must not touch public schema tables", db.lastSQL)
	}
}

func TestFetchThroughputPerMinute_QueryError(t *testing.T) {
	db := &fakeQueryDB{err: errors.New("connection reset")}
	q := NewQueries(db)

	if _, err := q.FetchThroughputPerMinute(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
}
