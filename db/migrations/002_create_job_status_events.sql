-- 002_create_job_status_events.sql
-- Job status events: time-series table for job lifecycle tracking.
-- Uses TimescaleDB hypertable for efficient time-range queries.

CREATE TABLE job_status_events (
  job_id UUID NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  event_data JSONB,
  pod_id VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

SELECT create_hypertable('job_status_events', 'event_timestamp', if_not_exists => TRUE);

CREATE INDEX idx_job_status_events_job_id ON job_status_events (job_id, event_timestamp DESC);
CREATE INDEX idx_job_status_events_timestamp ON job_status_events (event_timestamp DESC);
