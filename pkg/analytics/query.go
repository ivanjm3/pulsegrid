package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Queryer is the subset of *pgxpool.Pool used by Queries, allowing tests to
// substitute a fake in-memory implementation (same "borrow only what's
// needed" pattern as PostgresSink's DB interface).
type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Queries reads the four materialized views (task 38) for GET
// /analytics/summary (task 39). Read-only: every method here is a plain
// SELECT against a view under the analytics schema, never the public
// jobs/job_status_events tables.
type Queries struct {
	db Queryer
}

// NewQueries returns a Queries backed by db.
func NewQueries(db Queryer) *Queries {
	return &Queries{db: db}
}

// ThroughputPoint is one row of analytics.v_throughput_per_minute.
type ThroughputPoint struct {
	Minute        time.Time `json:"minute"`
	JobsCompleted int       `json:"jobs_completed"`
}

// FetchThroughputPerMinute returns the last 24h of completed-jobs-per-minute
// counts.
func (q *Queries) FetchThroughputPerMinute(ctx context.Context) ([]ThroughputPoint, error) {
	rows, err := q.db.Query(ctx, `SELECT minute, jobs_completed FROM analytics.v_throughput_per_minute`)
	if err != nil {
		return nil, fmt.Errorf("fetch throughput per minute: %w", err)
	}
	defer rows.Close()

	var out []ThroughputPoint
	for rows.Next() {
		var p ThroughputPoint
		if err := rows.Scan(&p.Minute, &p.JobsCompleted); err != nil {
			return nil, fmt.Errorf("fetch throughput per minute: scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch throughput per minute: rows: %w", err)
	}
	return out, nil
}

// LatencyPercentiles is one row of analytics.v_latency_percentiles.
type LatencyPercentiles struct {
	Hour time.Time `json:"hour"`
	P50  *float64  `json:"p50"`
	P95  *float64  `json:"p95"`
	P99  *float64  `json:"p99"`
}

// FetchLatencyPercentiles returns the last 7 days of per-hour p50/p95/p99
// transcode latency.
func (q *Queries) FetchLatencyPercentiles(ctx context.Context) ([]LatencyPercentiles, error) {
	rows, err := q.db.Query(ctx, `SELECT hour, p50, p95, p99 FROM analytics.v_latency_percentiles`)
	if err != nil {
		return nil, fmt.Errorf("fetch latency percentiles: %w", err)
	}
	defer rows.Close()

	var out []LatencyPercentiles
	for rows.Next() {
		var p LatencyPercentiles
		if err := rows.Scan(&p.Hour, &p.P50, &p.P95, &p.P99); err != nil {
			return nil, fmt.Errorf("fetch latency percentiles: scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch latency percentiles: rows: %w", err)
	}
	return out, nil
}

// FailureRateByClass is one row of analytics.v_failure_rate_by_class.
type FailureRateByClass struct {
	Hour           time.Time `json:"hour"`
	ErrorClass     *string   `json:"error_class"`
	FailureCount   int       `json:"failure_count"`
	FailureRatePct *float64  `json:"failure_rate_pct"`
}

// FetchFailureRateByClass returns the last 24h of per-hour, per-error-class
// failure counts and rates.
func (q *Queries) FetchFailureRateByClass(ctx context.Context) ([]FailureRateByClass, error) {
	rows, err := q.db.Query(ctx, `SELECT hour, error_class, failure_count, failure_rate_pct FROM analytics.v_failure_rate_by_class`)
	if err != nil {
		return nil, fmt.Errorf("fetch failure rate by class: %w", err)
	}
	defer rows.Close()

	var out []FailureRateByClass
	for rows.Next() {
		var f FailureRateByClass
		if err := rows.Scan(&f.Hour, &f.ErrorClass, &f.FailureCount, &f.FailureRatePct); err != nil {
			return nil, fmt.Errorf("fetch failure rate by class: scan: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch failure rate by class: rows: %w", err)
	}
	return out, nil
}

// RenditionBreakdown is one row of analytics.v_rendition_breakdown.
type RenditionBreakdown struct {
	RenditionID        *string  `json:"rendition_id"`
	CompletedCount     int      `json:"completed_count"`
	FailedCount        int      `json:"failed_count"`
	AvgDurationSeconds *float64 `json:"avg_duration_seconds"`
}

// FetchRenditionBreakdown returns the last 24h of per-rendition completion
// counts and average durations.
func (q *Queries) FetchRenditionBreakdown(ctx context.Context) ([]RenditionBreakdown, error) {
	rows, err := q.db.Query(ctx, `SELECT rendition_id, completed_count, failed_count, avg_duration_seconds FROM analytics.v_rendition_breakdown`)
	if err != nil {
		return nil, fmt.Errorf("fetch rendition breakdown: %w", err)
	}
	defer rows.Close()

	var out []RenditionBreakdown
	for rows.Next() {
		var r RenditionBreakdown
		if err := rows.Scan(&r.RenditionID, &r.CompletedCount, &r.FailedCount, &r.AvgDurationSeconds); err != nil {
			return nil, fmt.Errorf("fetch rendition breakdown: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch rendition breakdown: rows: %w", err)
	}
	return out, nil
}
