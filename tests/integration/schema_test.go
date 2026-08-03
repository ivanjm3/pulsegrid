//go:build integration

// Package integration holds tests that run against real external services
// (Postgres, Kafka, S3), gated behind the `integration` build tag per
// design.md's CI job ("go test -v -tags=integration ./tests/integration/...").
// Unlike the rest of the codebase's unit/property tests (which mock these
// services), these tests need a live database reachable at DATABASE_URL.
package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"pulsegrid/pkg"
	"pulsegrid/pkg/store"
)

// dsn returns the DATABASE_URL used by design.md's CI job, or skips the test
// if it isn't set (so `go test ./...` without -tags=integration, and without
// DATABASE_URL, never tries to dial a database that isn't there).
func dsn(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}
	return url
}

// TestMigrations_RunWithoutErrors runs the embedded migrations against a
// real database twice: once to apply them, once to confirm re-running is a
// no-op (RunMigrations treats migrate.ErrNoChange as success).
func TestMigrations_RunWithoutErrors(t *testing.T) {
	url := dsn(t)

	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations (first run): %v", err)
	}
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations (second run, should be no-op): %v", err)
	}
}

// TestMigrations_TablesHaveExpectedColumns verifies the jobs and
// job_status_events tables exist with the columns and types from
// db/migrations/001_create_jobs_table.up.sql and
// 002_create_job_status_events.up.sql.
func TestMigrations_TablesHaveExpectedColumns(t *testing.T) {
	url := dsn(t)
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	wantJobsColumns := map[string]string{
		"job_id":                 "uuid",
		"status":                 "character varying",
		"source_file_name":       "character varying",
		"source_file_size_bytes": "bigint",
		"source_s3_uri":          "text",
		"output_s3_prefix":       "text",
		"requested_renditions":   "jsonb",
		"submission_time":        "timestamp with time zone",
		"processing_start_time":  "timestamp with time zone",
		"completion_time":        "timestamp with time zone",
		"failure_reason":         "text",
		"retry_count":            "integer",
		"created_at":             "timestamp with time zone",
		"updated_at":             "timestamp with time zone",
	}
	assertColumns(t, db, "jobs", wantJobsColumns)

	wantEventColumns := map[string]string{
		"job_id":          "uuid",
		"event_type":      "character varying",
		"event_timestamp": "timestamp with time zone",
		"event_data":      "jsonb",
		"pod_id":          "character varying",
		"created_at":      "timestamp with time zone",
	}
	assertColumns(t, db, "job_status_events", wantEventColumns)
}

func assertColumns(t *testing.T, db *sql.DB, table string, want map[string]string) {
	t.Helper()

	rows, err := db.Query(`
		SELECT column_name, data_type FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
	`, table)
	if err != nil {
		t.Fatalf("query columns for %s: %v", table, err)
	}
	defer rows.Close()

	got := make(map[string]string)
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			t.Fatalf("scan column row: %v", err)
		}
		got[name] = dataType
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	for col, wantType := range want {
		gotType, ok := got[col]
		if !ok {
			t.Errorf("%s: missing column %q", table, col)
			continue
		}
		if gotType != wantType {
			t.Errorf("%s.%s: type = %q, want %q", table, col, gotType, wantType)
		}
	}
}

// TestMigrations_IndexesExist verifies the indexes declared in the migration
// files exist after RunMigrations.
func TestMigrations_IndexesExist(t *testing.T) {
	url := dsn(t)
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	wantIndexes := []string{
		"idx_jobs_status",
		"idx_jobs_submission_time",
		"idx_jobs_completion_time",
		"idx_job_status_events_job_id",
		"idx_job_status_events_timestamp",
	}

	rows, err := db.Query(`SELECT indexname FROM pg_indexes WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	for _, idx := range wantIndexes {
		if !existing[idx] {
			t.Errorf("missing index %q", idx)
		}
	}
}

// TestStore_InsertAndQueryByID is the query test task 25.1 asks for: insert
// a job, query it back by id, verify the result, against a real database
// (not the fakeDB used by pkg/store/postgres_test.go's unit tests).
func TestStore_InsertAndQueryByID(t *testing.T) {
	url := dsn(t)
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := store.Connect(ctx, url)
	if err != nil {
		t.Fatalf("store.Connect: %v", err)
	}
	defer pool.Close()

	s := store.NewStore(pool)

	id, err := pkg.NewJobID()
	if err != nil {
		t.Fatalf("NewJobID: %v", err)
	}
	job := pkg.Job{
		ID:                  id,
		SourceName:          "integration-test.mp4",
		SourceFileSizeBytes: 4096,
		SourceS3URI:         "s3://pulsegrid-source/" + id + "/original.mp4",
		OutputS3Prefix:      "s3://pulsegrid-output/" + id + "/",
		Status:              pkg.JobStatusSubmitted,
		Renditions:          []pkg.Rendition{{ID: "720p", Codec: "libx264", BitrateKbps: 5000, Width: 1280, Height: 720}},
		SubmissionTime:      time.Now().UTC().Truncate(time.Microsecond),
	}

	if err := s.RecordJobMetadata(ctx, job); err != nil {
		t.Fatalf("RecordJobMetadata: %v", err)
	}
	defer s.DeleteJob(ctx, job.ID)

	got, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.ID != job.ID {
		t.Errorf("ID = %q, want %q", got.ID, job.ID)
	}
	if got.Status != job.Status {
		t.Errorf("Status = %q, want %q", got.Status, job.Status)
	}
	if got.SourceName != job.SourceName {
		t.Errorf("SourceName = %q, want %q", got.SourceName, job.SourceName)
	}
	if len(got.Renditions) != 1 || got.Renditions[0] != job.Renditions[0] {
		t.Errorf("Renditions = %+v, want %+v", got.Renditions, job.Renditions)
	}
	if !got.SubmissionTime.Equal(job.SubmissionTime) {
		t.Errorf("SubmissionTime = %v, want %v", got.SubmissionTime, job.SubmissionTime)
	}

	if err := s.RecordStatusEvent(ctx, job.ID, "submitted", map[string]any{"source": "integration-test"}, "test-pod"); err != nil {
		t.Fatalf("RecordStatusEvent: %v", err)
	}
}
