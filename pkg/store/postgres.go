// Package store implements Postgres-backed persistence of job metadata and
// status events for the API server.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"pulsegrid/pkg"
)

// maxConnectAttempts and the backoff schedule match the design doc's retry
// policy for database connections: 1s, 2s, 4s, 8s, 16s (max 5 attempts).
const maxConnectAttempts = 5

var backoffSchedule = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// DB is the subset of *pgxpool.Pool used by Store, allowing tests to
// substitute a fake in-memory implementation.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store persists job metadata and status events to Postgres.
type Store struct {
	db DB
}

// NewStore returns a Store backed by db.
func NewStore(db DB) *Store {
	return &Store{db: db}
}

// Connect opens a connection pool to dsn, retrying transient connection
// errors with exponential backoff.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 0; attempt < maxConnectAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoffSchedule[attempt-1]):
			}
		}

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			lastErr = err
			continue
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			lastErr = err
			continue
		}
		return pool, nil
	}

	return nil, fmt.Errorf("connect to postgres: exhausted %d attempts: %w", maxConnectAttempts, lastErr)
}

// RecordJobMetadata inserts job into the jobs table with the given status.
func (s *Store) RecordJobMetadata(ctx context.Context, job pkg.Job) error {
	renditions, err := json.Marshal(job.Renditions)
	if err != nil {
		return fmt.Errorf("record job metadata: marshal renditions: %w", err)
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO jobs (
			job_id, status, source_file_name, source_file_size_bytes,
			source_s3_uri, output_s3_prefix, requested_renditions,
			submission_time, retry_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		job.ID, string(job.Status), job.SourceName, job.SourceFileSizeBytes,
		job.SourceS3URI, job.OutputS3Prefix, renditions,
		job.SubmissionTime.UTC(), job.RetryCount,
	)
	if err != nil {
		return fmt.Errorf("record job metadata: %w", err)
	}
	return nil
}

// UpdateJobStatus updates the status column for job_id, refreshing
// updated_at.
func (s *Store) UpdateJobStatus(ctx context.Context, jobID string, status pkg.JobStatus) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = $2, updated_at = CURRENT_TIMESTAMP WHERE job_id = $1
	`, jobID, string(status))
	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}
	return nil
}

// GetJob queries the jobs table by job_id and returns the stored record.
func (s *Store) GetJob(ctx context.Context, jobID string) (pkg.Job, error) {
	row := s.db.QueryRow(ctx, `
		SELECT job_id, status, source_file_name, source_file_size_bytes,
		       source_s3_uri, output_s3_prefix, requested_renditions,
		       submission_time, completion_time, retry_count
		FROM jobs WHERE job_id = $1
	`, jobID)

	var (
		job            pkg.Job
		status         string
		renditionsRaw  []byte
		completionTime *time.Time
	)
	if err := row.Scan(
		&job.ID, &status, &job.SourceName, &job.SourceFileSizeBytes,
		&job.SourceS3URI, &job.OutputS3Prefix, &renditionsRaw,
		&job.SubmissionTime, &completionTime, &job.RetryCount,
	); err != nil {
		return pkg.Job{}, fmt.Errorf("get job %s: %w", jobID, err)
	}

	job.Status = pkg.JobStatus(status)
	job.CompletionTime = completionTime
	if err := json.Unmarshal(renditionsRaw, &job.Renditions); err != nil {
		return pkg.Job{}, fmt.Errorf("get job %s: unmarshal renditions: %w", jobID, err)
	}
	return job, nil
}

// RecordStatusEvent inserts an event into the job_status_events table.
func (s *Store) RecordStatusEvent(ctx context.Context, jobID, eventType string, eventData map[string]any, podID string) error {
	var dataJSON []byte
	if eventData != nil {
		var err error
		dataJSON, err = json.Marshal(eventData)
		if err != nil {
			return fmt.Errorf("record status event: marshal event data: %w", err)
		}
	}

	_, err := s.db.Exec(ctx, `
		INSERT INTO job_status_events (job_id, event_type, event_timestamp, event_data, pod_id)
		VALUES ($1, $2, $3, $4, $5)
	`, jobID, eventType, time.Now().UTC(), dataJSON, podID)
	if err != nil {
		return fmt.Errorf("record status event: %w", err)
	}
	return nil
}
