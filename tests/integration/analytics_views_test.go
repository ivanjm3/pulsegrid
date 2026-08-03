//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"pulsegrid/pkg/analytics"
	"pulsegrid/pkg/store"
)

// insertLifecycleEvent inserts one row directly into
// analytics.job_lifecycle_events at eventTime, bypassing the Kafka/sink
// path so the view SQL can be tested against controlled, known inputs.
func insertLifecycleEvent(t *testing.T, db *sql.DB, jobID, eventType string, renditionID, errorClass *string, eventTime time.Time) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO analytics.job_lifecycle_events (job_id, event_type, rendition_id, error_class, pod_id, event_time)
		VALUES ($1, $2, $3, $4, 'test-pod', $5)
	`, jobID, eventType, renditionID, errorClass, eventTime)
	if err != nil {
		t.Fatalf("insert lifecycle event: %v", err)
	}
}

func setupAnalyticsDB(t *testing.T) *sql.DB {
	t.Helper()
	url := dsn(t)
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Isolate each test run: truncate rather than reuse rows from a
	// previous run in the same database.
	if _, err := db.Exec(`TRUNCATE analytics.job_lifecycle_events`); err != nil {
		t.Fatalf("truncate analytics.job_lifecycle_events: %v", err)
	}
	return db
}

func refreshAllViews(t *testing.T, db *sql.DB) {
	t.Helper()
	pool, err := store.Connect(context.Background(), dsn(t))
	if err != nil {
		t.Fatalf("store.Connect: %v", err)
	}
	defer pool.Close()

	r := analytics.NewRefresher(pool)
	if err := r.RefreshAll(context.Background()); err != nil {
		t.Fatalf("RefreshAll: %v", err)
	}
}

func strp(s string) *string { return &s }

// TestView_ThroughputPerMinute: 10 completed events spread one per minute
// over 10 minutes should produce 10 rows, each with jobs_completed = 1.
func TestView_ThroughputPerMinute(t *testing.T) {
	db := setupAnalyticsDB(t)
	base := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	for i := 0; i < 10; i++ {
		jobID := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		insertLifecycleEvent(t, db, jobID, "job_completed", nil, nil, base.Add(time.Duration(i)*time.Minute))
	}
	refreshAllViews(t, db)

	rows, err := db.Query(`SELECT jobs_completed FROM analytics.v_throughput_per_minute`)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var jobsCompleted int
		if err := rows.Scan(&jobsCompleted); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if jobsCompleted != 1 {
			t.Errorf("jobs_completed = %d, want 1", jobsCompleted)
		}
		count++
	}
	if count != 10 {
		t.Errorf("row count = %d, want 10", count)
	}
}

// TestView_LatencyPercentiles: jobs with a known, uniform duration should
// produce a p50 within 5% of that duration.
func TestView_LatencyPercentiles(t *testing.T) {
	db := setupAnalyticsDB(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	const wantDuration = 120 * time.Second

	for i := 0; i < 20; i++ {
		jobID := fmt.Sprintf("11111111-0000-0000-0000-%012d", i)
		start := hour.Add(time.Duration(i) * time.Second)
		insertLifecycleEvent(t, db, jobID, "job_started", nil, nil, start)
		insertLifecycleEvent(t, db, jobID, "job_completed", nil, nil, start.Add(wantDuration))
	}
	refreshAllViews(t, db)

	var p50 float64
	err := db.QueryRow(`SELECT p50 FROM analytics.v_latency_percentiles WHERE hour = $1`, hour).Scan(&p50)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	want := wantDuration.Seconds()
	if p50 < want*0.95 || p50 > want*1.05 {
		t.Errorf("p50 = %v, want within 5%% of %v", p50, want)
	}
}

// TestView_FailureRateByClass: 3 retryable + 1 permanent failure in the
// same hour should produce rates that sum to 100%.
func TestView_FailureRateByClass(t *testing.T) {
	db := setupAnalyticsDB(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(time.Minute)

	for i := 0; i < 3; i++ {
		jobID := fmt.Sprintf("22222222-0000-0000-0000-%012d", i)
		insertLifecycleEvent(t, db, jobID, "job_failed", nil, strp("retryable"), hour)
	}
	insertLifecycleEvent(t, db, "22222222-0000-0000-0000-999999999999", "job_failed", nil, strp("permanent"), hour)
	refreshAllViews(t, db)

	rows, err := db.Query(`SELECT error_class, failure_count, failure_rate_pct FROM analytics.v_failure_rate_by_class WHERE hour = date_trunc('hour', $1::timestamptz)`, hour)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	defer rows.Close()

	var total float64
	counts := map[string]int{}
	for rows.Next() {
		var errorClass string
		var failureCount int
		var rate float64
		if err := rows.Scan(&errorClass, &failureCount, &rate); err != nil {
			t.Fatalf("scan: %v", err)
		}
		counts[errorClass] = failureCount
		total += rate
	}
	if counts["retryable"] != 3 {
		t.Errorf("retryable failure_count = %d, want 3", counts["retryable"])
	}
	if counts["permanent"] != 1 {
		t.Errorf("permanent failure_count = %d, want 1", counts["permanent"])
	}
	if total < 99.9 || total > 100.1 {
		t.Errorf("failure_rate_pct total = %v, want ~100", total)
	}
}

// TestView_RenditionBreakdown: 5 720p completions + 2 job failures should
// report completed_count = 5 for the 720p row.
func TestView_RenditionBreakdown(t *testing.T) {
	db := setupAnalyticsDB(t)
	base := time.Now().UTC().Add(-time.Hour)

	for i := 0; i < 5; i++ {
		jobID := fmt.Sprintf("33333333-0000-0000-0000-%012d", i)
		start := base.Add(time.Duration(i) * time.Minute)
		insertLifecycleEvent(t, db, jobID, "job_started", nil, nil, start)
		insertLifecycleEvent(t, db, jobID, "rendition_completed", strp("720p"), nil, start.Add(10*time.Second))
	}
	for i := 0; i < 2; i++ {
		jobID := fmt.Sprintf("44444444-0000-0000-0000-%012d", i)
		insertLifecycleEvent(t, db, jobID, "job_failed", nil, strp("permanent"), base)
	}
	refreshAllViews(t, db)

	var completedCount int
	err := db.QueryRow(`SELECT completed_count FROM analytics.v_rendition_breakdown WHERE rendition_id = '720p'`).Scan(&completedCount)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	if completedCount != 5 {
		t.Errorf("completed_count = %d, want 5", completedCount)
	}
}
