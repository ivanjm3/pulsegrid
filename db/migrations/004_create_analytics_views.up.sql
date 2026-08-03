-- Four materialized views over analytics.job_lifecycle_events (task 38).
-- Materialized (not plain) views because the latency-percentile and
-- rendition-breakdown queries involve self-joins and percentile
-- aggregations that are too expensive to run live on every dashboard poll;
-- refreshed every 60s by the analytics-consumer's background goroutine.
--
-- Every view below has a UNIQUE index. REFRESH MATERIALIZED VIEW
-- CONCURRENTLY (required so a refresh never blocks concurrent reads from
-- the dashboard/API) fails outright without one -- Postgres has no way to
-- diff old vs. new rows for a concurrent swap otherwise. tasks.md's SQL
-- draft didn't include these; added them so the CONCURRENTLY refresh this
-- task explicitly asks for actually works.

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.v_throughput_per_minute AS
SELECT
  date_trunc('minute', event_time) AS minute,
  COUNT(*) AS jobs_completed
FROM analytics.job_lifecycle_events
WHERE event_type = 'job_completed'
  AND event_time > NOW() - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 1 DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_throughput_per_minute_minute
  ON analytics.v_throughput_per_minute (minute);

-- v_latency_percentiles pairs job_started + job_completed per job to
-- compute a per-hour transcode duration distribution.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.v_latency_percentiles AS
WITH durations AS (
  SELECT
    j.job_id,
    date_trunc('hour', s.event_time) AS hour,
    EXTRACT(EPOCH FROM (j.event_time - s.event_time)) AS duration_seconds
  FROM analytics.job_lifecycle_events j
  JOIN analytics.job_lifecycle_events s
    ON j.job_id = s.job_id
   AND j.event_type = 'job_completed'
   AND s.event_type = 'job_started'
  WHERE j.event_time > NOW() - INTERVAL '7 days'
)
SELECT
  hour,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_seconds) AS p50,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_seconds) AS p99
FROM durations GROUP BY 1 ORDER BY 1 DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_latency_percentiles_hour
  ON analytics.v_latency_percentiles (hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.v_failure_rate_by_class AS
SELECT
  date_trunc('hour', event_time) AS hour,
  error_class,
  COUNT(*) AS failure_count,
  ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY date_trunc('hour', event_time)), 0), 2) AS failure_rate_pct
FROM analytics.job_lifecycle_events
WHERE event_type = 'job_failed'
  AND event_time > NOW() - INTERVAL '24 hours'
GROUP BY 1, 2 ORDER BY 1 DESC, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_failure_rate_by_class_hour_class
  ON analytics.v_failure_rate_by_class (hour, error_class);

-- v_rendition_breakdown pairs each rendition_completed event with its job's
-- job_started event to compute per-rendition average duration.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.v_rendition_breakdown AS
SELECT
  c.rendition_id,
  COUNT(*) FILTER (WHERE c.event_type = 'rendition_completed') AS completed_count,
  (SELECT COUNT(*) FROM analytics.job_lifecycle_events f WHERE f.event_type = 'job_failed' AND f.event_time > NOW() - INTERVAL '24 hours') AS failed_count,
  AVG(EXTRACT(EPOCH FROM (c.event_time - s.event_time))) AS avg_duration_seconds
FROM analytics.job_lifecycle_events c
JOIN analytics.job_lifecycle_events s
  ON c.job_id = s.job_id
 AND c.event_type = 'rendition_completed'
 AND s.event_type = 'job_started'
WHERE c.event_time > NOW() - INTERVAL '24 hours'
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_rendition_breakdown_rendition_id
  ON analytics.v_rendition_breakdown (rendition_id);
