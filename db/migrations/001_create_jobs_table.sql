-- 001_create_jobs_table.sql
-- Jobs table: stores transcoding job metadata.

CREATE TABLE jobs (
  job_id UUID PRIMARY KEY,
  status VARCHAR(20) NOT NULL CHECK (status IN ('submitted', 'processing', 'completed', 'failed')),
  source_file_name VARCHAR(1024) NOT NULL,
  source_file_size_bytes BIGINT NOT NULL,
  source_s3_uri TEXT NOT NULL,
  output_s3_prefix TEXT NOT NULL,
  requested_renditions JSONB NOT NULL,
  submission_time TIMESTAMP NOT NULL,
  processing_start_time TIMESTAMP,
  completion_time TIMESTAMP,
  failure_reason TEXT,
  retry_count INTEGER DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_jobs_status ON jobs (status);
CREATE INDEX idx_jobs_submission_time ON jobs (submission_time DESC);
CREATE INDEX idx_jobs_completion_time ON jobs (completion_time DESC);
