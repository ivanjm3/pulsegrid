package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxHandle represents an in-flight database transaction for atomic job submission.
// Caller must call Commit or Rollback exactly once.
type TxHandle interface {
	// UpdateStatusAndCommit updates job status within the transaction and commits.
	UpdateStatusAndCommit(ctx context.Context, jobID string, status JobStatus) error
	// Rollback aborts the transaction. Safe to call after commit (no-op).
	Rollback(ctx context.Context) error
}

// JobFilter holds query parameters for filtering jobs.
type JobFilter struct {
	SubmittedAfter  *time.Time
	SubmittedBefore *time.Time
	Statuses        []JobStatus
	Limit           int
	Offset          int
}

// JobSummary is a lightweight job representation for list queries.
type JobSummary struct {
	JobID           string     `json:"job_id"`
	Status          JobStatus  `json:"status"`
	SubmissionTime  time.Time  `json:"submission_time"`
	CompletionTime  *time.Time `json:"completion_time,omitempty"`
	DurationSeconds *float64   `json:"duration_seconds,omitempty"`
}

// JobListResult holds paginated query results.
type JobListResult struct {
	Jobs   []JobSummary `json:"jobs"`
	Total  int          `json:"total"`
	Limit  int          `json:"limit"`
	Offset int          `json:"offset"`
}

// DBClient is the interface for Postgres job persistence.
// Allows nil-check for local dev and mocking in tests.
type DBClient interface {
	RecordJobMetadata(ctx context.Context, job Job) error
	RecordStatusEvent(ctx context.Context, jobID string, eventType string) error
	GetJobByID(ctx context.Context, jobID string) (Job, error)
	QueryJobs(ctx context.Context, filter JobFilter) (JobListResult, error)
	// InsertJobTx begins a transaction, inserts job with given status, returns TxHandle.
	// Caller uses TxHandle to update status + commit or rollback.
	InsertJobTx(ctx context.Context, job Job) (TxHandle, error)
	Ping(ctx context.Context) error
	Close()
}

// PostgresClient implements DBClient using pgxpool.
type PostgresClient struct {
	pool *pgxpool.Pool
}

// NewPostgresClient connects to Postgres via DATABASE_URL with retry.
// Uses pgxpool for connection pooling. Retries initial connection with backoff.
func NewPostgresClient(ctx context.Context) (*PostgresClient, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable not set")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}

	var pool *pgxpool.Pool
	err = RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		p, connErr := pgxpool.NewWithConfig(ctx, config)
		if connErr != nil {
			return connErr
		}
		// Verify connectivity
		if pingErr := p.Ping(ctx); pingErr != nil {
			p.Close()
			return pingErr
		}
		pool = p
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}

	return &PostgresClient{pool: pool}, nil
}

// RecordJobMetadata inserts job metadata into the jobs table.
func (c *PostgresClient) RecordJobMetadata(ctx context.Context, job Job) error {
	renditionsJSON, err := json.Marshal(job.Renditions)
	if err != nil {
		return fmt.Errorf("marshal renditions: %w", err)
	}

	query := `
		INSERT INTO jobs (
			job_id, status, source_file_name, source_file_size_bytes,
			source_s3_uri, output_s3_prefix, requested_renditions,
			submission_time, processing_start_time, completion_time,
			failure_reason, retry_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	_, err = c.pool.Exec(ctx, query,
		job.JobID,
		string(job.Status),
		job.SourceFileName,
		job.SourceFileSizeBytes,
		job.SourceS3URI,
		job.OutputS3Prefix,
		renditionsJSON,
		job.SubmissionTime,
		job.ProcessingStartTime,
		job.CompletionTime,
		job.FailureReason,
		job.RetryCount,
	)
	if err != nil {
		return fmt.Errorf("insert job %s: %w", job.JobID, err)
	}

	return nil
}

// GetJobByID queries the jobs table by job_id and returns the Job struct.
func (c *PostgresClient) GetJobByID(ctx context.Context, jobID string) (Job, error) {
	query := `
		SELECT job_id, status, source_file_name, source_file_size_bytes,
			source_s3_uri, output_s3_prefix, requested_renditions,
			submission_time, processing_start_time, completion_time,
			failure_reason, retry_count
		FROM jobs WHERE job_id = $1`

	var job Job
	var renditionsJSON []byte
	err := c.pool.QueryRow(ctx, query, jobID).Scan(
		&job.JobID,
		&job.Status,
		&job.SourceFileName,
		&job.SourceFileSizeBytes,
		&job.SourceS3URI,
		&job.OutputS3Prefix,
		&renditionsJSON,
		&job.SubmissionTime,
		&job.ProcessingStartTime,
		&job.CompletionTime,
		&job.FailureReason,
		&job.RetryCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, fmt.Errorf("get job %s: %w", jobID, err)
	}

	if err := json.Unmarshal(renditionsJSON, &job.Renditions); err != nil {
		return Job{}, fmt.Errorf("unmarshal renditions for job %s: %w", jobID, err)
	}

	return job, nil
}

// QueryJobs queries jobs with filters and returns paginated results.
func (c *PostgresClient) QueryJobs(ctx context.Context, filter JobFilter) (JobListResult, error) {
	// Build WHERE clauses dynamically.
	conditions := []string{}
	args := []interface{}{}
	argIdx := 1

	if filter.SubmittedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("submission_time >= $%d", argIdx))
		args = append(args, *filter.SubmittedAfter)
		argIdx++
	}
	if filter.SubmittedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("submission_time <= $%d", argIdx))
		args = append(args, *filter.SubmittedBefore)
		argIdx++
	}
	if len(filter.Statuses) > 0 {
		placeholders := []string{}
		for _, s := range filter.Statuses {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, string(s))
			argIdx++
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ", ")))
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total matching rows.
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereClause)
	var total int
	if err := c.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return JobListResult{}, fmt.Errorf("count jobs: %w", err)
	}

	// Fetch paginated results.
	dataQuery := fmt.Sprintf(
		"SELECT job_id, status, submission_time, completion_time FROM jobs %s ORDER BY submission_time DESC LIMIT $%d OFFSET $%d",
		whereClause, argIdx, argIdx+1,
	)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := c.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return JobListResult{}, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	jobs := []JobSummary{}
	for rows.Next() {
		var js JobSummary
		if err := rows.Scan(&js.JobID, &js.Status, &js.SubmissionTime, &js.CompletionTime); err != nil {
			return JobListResult{}, fmt.Errorf("scan job row: %w", err)
		}
		// Calculate duration if completed.
		if js.CompletionTime != nil {
			dur := js.CompletionTime.Sub(js.SubmissionTime).Seconds()
			js.DurationSeconds = &dur
		}
		jobs = append(jobs, js)
	}
	if err := rows.Err(); err != nil {
		return JobListResult{}, fmt.Errorf("iterate job rows: %w", err)
	}

	return JobListResult{
		Jobs:   jobs,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

// RecordStatusEvent inserts a status event into job_status_events table.
func (c *PostgresClient) RecordStatusEvent(ctx context.Context, jobID string, eventType string) error {
	query := `
		INSERT INTO job_status_events (job_id, event_type, event_timestamp)
		VALUES ($1, $2, $3)`

	_, err := c.pool.Exec(ctx, query, jobID, eventType, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert status event [job=%s, event=%s]: %w", jobID, eventType, err)
	}

	return nil
}

// Ping checks Postgres connectivity via the connection pool.
func (c *PostgresClient) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Pool returns the underlying pgxpool.Pool for direct access (e.g., migrations).
func (c *PostgresClient) Pool() *pgxpool.Pool {
	return c.pool
}

// Close closes the connection pool.
func (c *PostgresClient) Close() {
	c.pool.Close()
}

// pgTxHandle implements TxHandle using a pgx transaction.
type pgTxHandle struct {
	tx   pgx.Tx
	done bool
}

// UpdateStatusAndCommit updates job status within the transaction and commits.
func (h *pgTxHandle) UpdateStatusAndCommit(ctx context.Context, jobID string, status JobStatus) error {
	if h.done {
		return fmt.Errorf("transaction already finalized")
	}
	_, err := h.tx.Exec(ctx, `UPDATE jobs SET status = $1 WHERE job_id = $2`, string(status), jobID)
	if err != nil {
		return fmt.Errorf("update job %s status: %w", jobID, err)
	}
	if err := h.tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job %s: %w", jobID, err)
	}
	h.done = true
	return nil
}

// Rollback aborts the transaction. No-op if already finalized.
func (h *pgTxHandle) Rollback(ctx context.Context) error {
	if h.done {
		return nil
	}
	h.done = true
	return h.tx.Rollback(ctx)
}

// InsertJobTx begins a transaction, inserts job with status='submitting', returns TxHandle.
func (c *PostgresClient) InsertJobTx(ctx context.Context, job Job) (TxHandle, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx for job %s: %w", job.JobID, err)
	}

	renditionsJSON, err := json.Marshal(job.Renditions)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("marshal renditions: %w", err)
	}

	query := `
		INSERT INTO jobs (
			job_id, status, source_file_name, source_file_size_bytes,
			source_s3_uri, output_s3_prefix, requested_renditions,
			submission_time, processing_start_time, completion_time,
			failure_reason, retry_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	_, err = tx.Exec(ctx, query,
		job.JobID,
		string(JobStatusSubmitting),
		job.SourceFileName,
		job.SourceFileSizeBytes,
		job.SourceS3URI,
		job.OutputS3Prefix,
		renditionsJSON,
		job.SubmissionTime,
		job.ProcessingStartTime,
		job.CompletionTime,
		job.FailureReason,
		job.RetryCount,
	)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("insert job %s: %w", job.JobID, err)
	}

	return &pgTxHandle{tx: tx}, nil
}
