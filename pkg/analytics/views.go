package analytics

import (
	"context"
	"fmt"
	"log"
	"time"
)

// RefreshInterval is how often the analytics-consumer refreshes the
// materialized views, per task 38 ("every 60 seconds").
const RefreshInterval = 60 * time.Second

// views lists every materialized view Refresher refreshes, in a fixed
// order (independent views, so order doesn't affect correctness -- fixed
// only for deterministic logging/tests).
var views = []string{
	"analytics.v_throughput_per_minute",
	"analytics.v_latency_percentiles",
	"analytics.v_failure_rate_by_class",
	"analytics.v_rendition_breakdown",
}

// Refresher periodically runs REFRESH MATERIALIZED VIEW CONCURRENTLY
// against the four analytics views (task 38).
type Refresher struct {
	db DB
}

// NewRefresher returns a Refresher backed by db.
func NewRefresher(db DB) *Refresher {
	return &Refresher{db: db}
}

// RefreshAll refreshes every view, returning the first error encountered
// (after attempting the rest, so one bad view doesn't skip the others).
func (r *Refresher) RefreshAll(ctx context.Context) error {
	var firstErr error
	for _, view := range views {
		if err := r.refreshOne(ctx, view); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (r *Refresher) refreshOne(ctx context.Context, view string) error {
	_, err := r.db.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", view))
	if err != nil {
		return fmt.Errorf("refresh %s: %w", view, err)
	}
	return nil
}

// RunLoop calls RefreshAll every RefreshInterval until ctx is cancelled.
// A refresh failure is logged and never stops the loop -- a transient
// refresh error shouldn't take the whole background job down, since the
// next tick will simply try again.
func (r *Refresher) RunLoop(ctx context.Context) {
	ticker := time.NewTicker(RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.RefreshAll(ctx); err != nil {
				log.Printf("event=analytics_view_refresh_error error=%v", err)
			}
		}
	}
}
