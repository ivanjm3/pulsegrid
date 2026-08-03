CREATE TABLE IF NOT EXISTS job_status_events (
  job_id UUID NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  event_data JSONB,
  pod_id VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Convert to TimescaleDB hypertable for time-series optimization, if the
-- extension is available. Amazon RDS for PostgreSQL does not support the
-- timescaledb extension (only self-managed Postgres/Timescale Cloud do), so
-- this degrades to a plain indexed table on RDS rather than failing the
-- migration outright.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    PERFORM create_hypertable('job_status_events', 'event_timestamp', if_not_exists => TRUE);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_job_status_events_job_id ON job_status_events (job_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_job_status_events_timestamp ON job_status_events (event_timestamp DESC);
