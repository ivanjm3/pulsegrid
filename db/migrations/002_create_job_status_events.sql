CREATE TABLE IF NOT EXISTS job_status_events (
  job_id UUID NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  event_data JSONB,
  pod_id VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Convert to TimescaleDB hypertable for time-series optimization.
SELECT create_hypertable('job_status_events', 'event_timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_job_status_events_job_id ON job_status_events (job_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_job_status_events_timestamp ON job_status_events (event_timestamp DESC);
