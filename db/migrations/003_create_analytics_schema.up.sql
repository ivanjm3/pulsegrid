-- Analytics schema, isolated from `public` so analytics aggregation queries
-- never take table-level locks on or cause autovacuum pressure against the
-- operational `jobs`/`job_status_events` tables (task 37).
CREATE SCHEMA IF NOT EXISTS analytics;

-- NOTE: the primary key here is (id, event_time), not id alone. TimescaleDB
-- requires every unique constraint on a hypertable (including the primary
-- key) to include the partitioning column ("event_time"); a bare
-- `id BIGSERIAL PRIMARY KEY` fails create_hypertable() below with
-- "cannot create a unique index without the column event_time (used in
-- partitioning)". id keeps its own UNIQUE constraint via the BIGSERIAL
-- sequence default, so it's still a stable per-row identifier — only the
-- constraint shape changes to satisfy the hypertable requirement, verified
-- against a real timescaledb/timescaledb container (see .spec/CHANGELOG.md).
CREATE TABLE IF NOT EXISTS analytics.job_lifecycle_events (
  id           BIGSERIAL NOT NULL,
  job_id       UUID NOT NULL,
  event_type   VARCHAR(30) NOT NULL,
  rendition_id VARCHAR(20),
  error_class  VARCHAR(20),
  error_reason TEXT,
  pod_id       VARCHAR(255) NOT NULL,
  event_time   TIMESTAMPTZ NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, event_time)
);

CREATE INDEX IF NOT EXISTS idx_ale_job_id      ON analytics.job_lifecycle_events (job_id);
CREATE INDEX IF NOT EXISTS idx_ale_event_time  ON analytics.job_lifecycle_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_ale_event_type  ON analytics.job_lifecycle_events (event_type);

-- Convert to a TimescaleDB hypertable on event_time, consistent with the
-- operational job_status_events pattern (migration 002). Same graceful
-- degradation: Amazon RDS for PostgreSQL doesn't support the timescaledb
-- extension, so this is a no-op plain-table fallback there.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    PERFORM create_hypertable('analytics.job_lifecycle_events', 'event_time', if_not_exists => TRUE);
  END IF;
END $$;
