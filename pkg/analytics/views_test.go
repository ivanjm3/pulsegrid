package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRefreshDB records every REFRESH statement issued, and fails on
// demand for a named view.
type fakeRefreshDB struct {
	calls   []string
	failOn  string
	failErr error
}

func (f *fakeRefreshDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, sql)
	if f.failOn != "" && strings.Contains(sql, f.failOn) {
		return pgconn.CommandTag{}, f.failErr
	}
	return pgconn.CommandTag{}, nil
}

func TestRefresher_RefreshAll_RefreshesAllFourViewsConcurrently(t *testing.T) {
	db := &fakeRefreshDB{}
	r := NewRefresher(db)

	if err := r.RefreshAll(context.Background()); err != nil {
		t.Fatalf("RefreshAll: unexpected error: %v", err)
	}

	wantViews := []string{
		"analytics.v_throughput_per_minute",
		"analytics.v_latency_percentiles",
		"analytics.v_failure_rate_by_class",
		"analytics.v_rendition_breakdown",
	}
	if len(db.calls) != len(wantViews) {
		t.Fatalf("Exec called %d times, want %d", len(db.calls), len(wantViews))
	}
	for i, call := range db.calls {
		if !strings.HasPrefix(call, "REFRESH MATERIALIZED VIEW CONCURRENTLY") {
			t.Errorf("call[%d] = %q, want REFRESH MATERIALIZED VIEW CONCURRENTLY prefix", i, call)
		}
		if !strings.Contains(call, wantViews[i]) {
			t.Errorf("call[%d] = %q, want to contain %q", i, call, wantViews[i])
		}
	}
}

func TestRefresher_RefreshAll_OneViewFails_OthersStillAttempted(t *testing.T) {
	db := &fakeRefreshDB{failOn: "v_latency_percentiles", failErr: errors.New("lock timeout")}
	r := NewRefresher(db)

	err := r.RefreshAll(context.Background())
	if err == nil {
		t.Fatal("RefreshAll: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "v_latency_percentiles") {
		t.Errorf("error = %v, want to mention v_latency_percentiles", err)
	}
	if len(db.calls) != 4 {
		t.Errorf("Exec called %d times, want 4 (all views attempted despite one failure)", len(db.calls))
	}
}

func TestRefresher_RunLoop_StopsOnContextCancellation(t *testing.T) {
	db := &fakeRefreshDB{}
	r := NewRefresher(db)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.RunLoop(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not return within 2s of context cancellation")
	}
}
