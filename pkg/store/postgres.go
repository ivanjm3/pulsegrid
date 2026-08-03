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

// maxConnectAttempts and connectBaseDelay match the design doc's retry
// policy for database connections: 1s, 2s, 4s, 8s, 16s (max 5 attempts).
const (
	maxConnectAttempts = 5
	connectBaseDelay   = 1 * time.Second
)

// DB is the subset of *pgxpool.Pool used by Store, allowing tests to
// substitute a fake in-memory implementation.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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
	var pool *pgxpool.Pool
	err := pkg.RetryWithBackoff(ctx, maxConnectAttempts, connectBaseDelay, time.Sleep, func(ctx context.Context) error {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return err
		}
		if err := p.Ping(ctx); err != nil {
			p.Close()
			return err
		}
		pool = p
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return pool, nil
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

// MarkJobProcessing transitions job jobID to the processing status. Called
// by the worker when it begins handling a job, so GET /jobs/{id} reflects
// that the job left the queue instead of appearing "submitted" forever.
func (s *Store) MarkJobProcessing(ctx context.Context, jobID string) error {
	return s.UpdateJobStatus(ctx, jobID, pkg.JobStatusProcessing)
}

// MarkJobCompleted transitions job jobID to completed and records
// completion_time, per Requirement 5.2 ("update the job record with
// completion timestamp").
func (s *Store) MarkJobCompleted(ctx context.Context, jobID string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = $2, completion_time = $3, updated_at = CURRENT_TIMESTAMP WHERE job_id = $1
	`, jobID, string(pkg.JobStatusCompleted), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("mark job completed: %w", err)
	}
	return nil
}

// MarkJobFailed transitions job jobID to failed, recording failureReason and
// retryCount, per Requirement 5.3 ("update the job record with failure
// timestamp, failure reason, retry count"). completion_time is reused as the
// terminal timestamp — the jobs table (design.md, task 5) has no separate
// failure_time column, and completion_time is otherwise unset for a job that
// never succeeded.
func (s *Store) MarkJobFailed(ctx context.Context, jobID, failureReason string, retryCount int) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = $2, failure_reason = $3, retry_count = $4, completion_time = $5, updated_at = CURRENT_TIMESTAMP WHERE job_id = $1
	`, jobID, string(pkg.JobStatusFailed), failureReason, retryCount, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("mark job failed: %w", err)
	}
	return nil
}

// DeleteJob removes the jobs row for jobID. Used to roll back an orphaned
// "submitting" row when the subsequent Kafka publish fails, so the job never
// existed from the client's point of view.
func (s *Store) DeleteJob(ctx context.Context, jobID string) error {
	_, err := s.db.Exec(ctx, `DELETE FROM jobs WHERE job_id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("delete job %s: %w", jobID, err)
	}
	return nil
}

// GetJob queries the jobs table by job_id and returns the stored record.
func (s *Store) GetJob(ctx context.Context, jobID string) (pkg.Job, error) {
	row := s.db.QueryRow(ctx, `
		SELECT job_id, status, source_file_name, source_file_size_bytes,
		       source_s3_uri, output_s3_prefix, requested_renditions,
		       submission_time, completion_time, retry_count, failure_reason
		FROM jobs WHERE job_id = $1
	`, jobID)

	var (
		job            pkg.Job
		status         string
		renditionsRaw  []byte
		completionTime *time.Time
		failureReason  *string
	)
	if err := row.Scan(
		&job.ID, &status, &job.SourceName, &job.SourceFileSizeBytes,
		&job.SourceS3URI, &job.OutputS3Prefix, &renditionsRaw,
		&job.SubmissionTime, &completionTime, &job.RetryCount, &failureReason,
	); err != nil {
		return pkg.Job{}, fmt.Errorf("get job %s: %w", jobID, err)
	}

	job.Status = pkg.JobStatus(status)
	job.CompletionTime = completionTime
	job.FailureReason = failureReason
	if err := json.Unmarshal(renditionsRaw, &job.Renditions); err != nil {
		return pkg.Job{}, fmt.Errorf("get job %s: unmarshal renditions: %w", jobID, err)
	}
	return job, nil
}

// JobFilter constrains a ListJobs query.
type JobFilter struct {
	SubmittedAfter  *time.Time
	SubmittedBefore *time.Time
	Statuses        []pkg.JobStatus
	Limit           int
	Offset          int
}

// JobSummary is a single row of a ListJobs result.
type JobSummary struct {
	ID             string
	Status         pkg.JobStatus
	SubmissionTime time.Time
	CompletionTime *time.Time
}

// ListJobs queries the jobs table for rows matching filter, ordered by
// submission_time descending, along with the total count of matching rows
// (ignoring limit/offset).
func (s *Store) ListJobs(ctx context.Context, filter JobFilter) ([]JobSummary, int, error) {
	where := []string{"1=1"}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if filter.SubmittedAfter != nil {
		where = append(where, "submission_time >= "+arg(filter.SubmittedAfter.UTC()))
	}
	if filter.SubmittedBefore != nil {
		where = append(where, "submission_time <= "+arg(filter.SubmittedBefore.UTC()))
	}
	if len(filter.Statuses) > 0 {
		statuses := make([]string, len(filter.Statuses))
		for i, st := range filter.Statuses {
			statuses[i] = string(st)
		}
		where = append(where, "status = ANY("+arg(statuses)+")")
	}
	whereClause := ""
	for i, w := range where {
		if i > 0 {
			whereClause += " AND "
		}
		whereClause += w
	}

	var total int
	countSQL := "SELECT COUNT(*) FROM jobs WHERE " + whereClause
	if err := s.db.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("list jobs: count: %w", err)
	}

	limitArg := arg(filter.Limit)
	offsetArg := arg(filter.Offset)
	querySQL := "SELECT job_id, status, submission_time, completion_time FROM jobs WHERE " + whereClause +
		" ORDER BY submission_time DESC LIMIT " + limitArg + " OFFSET " + offsetArg

	rows, err := s.db.Query(ctx, querySQL, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: query: %w", err)
	}
	defer rows.Close()

	var results []JobSummary
	for rows.Next() {
		var (
			js             JobSummary
			status         string
			completionTime *time.Time
		)
		if err := rows.Scan(&js.ID, &status, &js.SubmissionTime, &completionTime); err != nil {
			return nil, 0, fmt.Errorf("list jobs: scan: %w", err)
		}
		js.Status = pkg.JobStatus(status)
		js.CompletionTime = completionTime
		results = append(results, js)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("list jobs: rows: %w", err)
	}

	return results, total, nil
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
