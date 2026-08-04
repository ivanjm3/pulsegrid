# Implementation Plan: Pulsegrid Distributed Video Transcoding Platform

## Overview

Convert design into incremental, testable Go implementation steps. Each task builds on previous steps, integrating incrementally. Property-based tests validate core logic; unit tests cover edge cases. Structure follows component layers: core types → API server → job queue integration → worker pods → infrastructure orchestration.

## Tasks

- [x] 1. Project scaffolding and core types
  - Set up Go module structure: `pulsegrid/` with subdirs `cmd/api`, `cmd/worker`, `pkg/`
  - Create core type definitions: `Job`, `Rendition`, `JobStatus`, `RetryConfig`
  - Implement UUID v4 Job ID generation (stdlib `crypto/rand`)
  - Define error types: `TranscodingError`, `ResourceConstraintError`
  - _Requirements: 1.4, 2.1_

- [x]\* 1.1 Write property test for Job ID generation
  - **Property 1: Job ID Uniqueness and Format**
  - **Validates: Requirements 1.4**
  - Generate 100+ random upload contexts, verify all Job IDs unique and RFC 4122 v4 format
  - Use `quick.Check` or `gopter` for property generation

- [x] 2. API Server: HTTP server and request parsing
  - Create `cmd/api/main.go` with HTTP server on port 8080
  - Implement multipart/form-data parsing for POST `/videos/upload`
  - Extract: video file, source_name, renditions JSON
  - Implement input validation: file size limit (10GB), rendition schema validation
  - Return 400/413 errors with structured error responses
  - _Requirements: 1.1, 1.2, 1.3_

- [x] 2.1 Write unit tests for HTTP request validation
  - Test valid multipart request with required fields
  - Test missing source_name field → 400 Bad Request
  - Test file exceeds 10GB → 413 Payload Too Large
  - Test invalid rendition JSON → 400 Bad Request

- [x] 3. API Server: S3 integration for source upload
  - Implement S3 client (AWS SDK v2 Go)
  - Create function: `uploadSourceToS3(ctx, file, jobID)` → s3://pulsegrid-source/{jobID}/original.mp4
  - Implement multipart S3 upload (no local disk buffering)
  - Tag objects: job_id, upload_time, source_name
  - Handle S3 errors with exponential backoff (1s, 2s, 4s, 8s, 16s max 5 attempts)
  - _Requirements: 1.5, 1.6_

- [x] 3.1 Write unit tests for S3 upload
  - Mock S3 client (use `aws/smithy-go` test utilities)
  - Test successful upload with correct tagging
  - Test S3 transient error → retry with backoff
  - Test S3 permanent error (AccessDenied) → return 500

- [x] 4. API Server: Kafka job queue integration
  - Implement Kafka producer (segment go library)
  - Create function: `enqueueJob(ctx, job)` → publishes to topic `transcoding-jobs`
  - Message schema: Job_ID, source_s3_uri, renditions, output_s3_prefix, retry_count, timestamps
  - Serialize to JSON, partition by job_id hash
  - Handle Kafka errors with retry (same backoff as S3)
  - _Requirements: 2.1, 1.6_

- [x] 4.1 Write property test for Kafka message schema
  - **Property 2: Kafka Job Message Schema Compliance**
  - **Validates: Requirements 2.1, 1.6**
  - Generate random rendition specs (0-5 renditions, varied codecs/bitrates)
  - Publish to Kafka, consume, verify JSON structure and required fields
  - Verify no missing fields, correct types

- [x] 5. API Server: Postgres integration for job tracking
  - Create database connection pool (pgx driver)
  - Create database schema: `jobs` and `job_status_events` tables (from design)
  - Implement function: `recordJobMetadata(ctx, job)` → INSERT into jobs table
  - Implement function: `recordStatusEvent(ctx, jobID, eventType)` → INSERT into job_status_events
  - Handle connection errors, retry pool
  - _Requirements: 5.1, 5.2_

- [x] 5.1 Write property test for database round-trip
  - **Property 7: Database Persistence and Round-Trip**
  - **Validates: Requirements 5.1, 5.5**
  - Generate random jobs, insert via `recordJobMetadata`, query by job_id
  - Verify returned record matches all input fields (job_id, status, submission_time, renditions)
  - Test with 50+ random jobs

- [x] 6. API Server: Complete /videos/upload endpoint (atomic DB-Kafka ordering)
  - **CRITICAL: Write order to prevent orphans**
    1. Begin DB transaction: INSERT into jobs table with status='submitting'
    2. Publish to Kafka (outside tx, will see submitting status if query before Kafka commit)
    3. UPDATE jobs set status='submitted' (mark confirmed in queue)
    4. Commit DB transaction
  - If Kafka publish fails: rollback DB, return 500 (job never existed from client view)
  - If DB commit fails after Kafka publish: log alert (job in queue but not in DB) — operator must investigate
  - Wire full flow: parse → validate → S3 upload → (1) DB insert → (2) Kafka enqueue → (3) DB update
  - Return JSON: job_id, status_uri, estimated_wait_time_seconds, submission_time
  - Add request_id generation for tracing
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

- [x]\* 6.1 Write unit test for DB-Kafka write order
  - Mock Kafka publish fails → verify DB transaction rolled back, job not inserted
  - Mock DB update fails after Kafka → verify alert logged, orphan flag set

- [x] 7. API Server: GET /jobs/{job_id} status query endpoint
  - Implement database query: fetch job by ID
  - Query job_status_events for latest status
  - If completed: fetch output file list from S3 or manifest
  - Return JSON: job_id, status, submission_time, completion_time, output_files array
  - Return 404 if job not found
  - _Requirements: 5.5_

- [x]\* 7.1 Write unit tests for status query
  - Test completed job → returns status, completion_time, output_files
  - Test processing job → returns status, no completion_time
  - Test nonexistent job → 404 Not Found
  - Test failed job → returns status, failure_reason

- [x] 8. API Server: GET /jobs range query endpoint
  - Implement query: `/jobs?submitted_after=<ts>&submitted_before=<ts>&status=<list>&limit=<n>&offset=<m>`
  - Parse timestamps (ISO 8601), validate ranges
  - Query Postgres for jobs matching filters
  - Return paginated results with total count
  - _Requirements: 5.6_

- [x] 9. API Server: Prometheus metrics and /metrics endpoint (part 1 - submit/duration only)
  - ~~Initialize Prometheus client library (prometheus/client_golang)~~ ✓
  - ~~Define metrics: `pulsegrid_jobs_submitted_total` (counter), `pulsegrid_upload_duration_seconds` (histogram)~~ ✓
  - ~~Implement /metrics endpoint (OpenMetrics format)~~ ✓ (route registered on dedicated :8081 listener)
  - ~~Emit metrics in /videos/upload handler: record submission count, upload duration~~ ✓
  - **DO NOT** query Kafka consumer lag yet (worker consumer doesn't exist until task 12)
  - _Requirements: 8.1, 8.2 (partial)_

- [x]\* 9.1 Write unit tests for metrics emission
  - Mock Prometheus registry, emit events, verify counters incremented
  - Verify histogram buckets correct (upload_duration_seconds)

- [x] 9.2. API Server: Queue depth gauge (moved to wave 4, after task 12)
  - Query Kafka admin API (not consumer lag): list partitions, fetch offsets
  - Calculate queue_depth = sum of partition lags across `transcoding-jobs` topic
  - Update `pulsegrid_queue_depth_jobs` gauge every 30s
  - _Requirements: 8.2 (deferred)_

- [x] 10. API Server: Health checks and liveness/readiness
  - Implement GET /health endpoint
  - Check: Kafka broker reachable, Postgres connection alive, S3 connectivity
  - Return 200 if all healthy, 503 if any down
  - Used by Kubernetes liveness/readiness probes
  - _Requirements: 6 (implicit)_

- [x] 11. Checkpoint - API Server functional tests
  - Run all endpoint tests end-to-end with mocked Kafka/S3/Postgres
  - Verify upload flow: parse → S3 → Kafka → DB → 202 response
  - Verify status query: job created → job queried → status correct
  - Ask user if questions arise.

- [x] 12. Worker Pod: Core job processing loop and Kafka consumer (correct Kafka semantics)
  - Create `cmd/worker/main.go`
  - Initialize Kafka consumer: topic `transcoding-jobs`, group `pulsegrid-workers`, auto.offset.reset=earliest
  - **Kafka semantics (NOT SQS)**: No message lock/visibility timeout. Consumer joins group, reads partition offsets.
    - Offset commit = mark as processed. If worker crashes before commit, another consumer in group gets same message.
    - Session timeout (default 30s): if worker dies, broker detects dead consumer after timeout, rebalance, offset reprocessed.
    - To handle slow jobs: set session.timeout.ms=1800000 (30 min) so long transcode doesn't trigger timeout.
  - Implement polling loop: `for { msg := consumer.Poll(ctx) → process job → on success: consumer.CommitOffsets(msg) }`
  - Handle SIGTERM: call consumer.Close() gracefully, finish any in-flight job before close, don't start new jobs after signal
  - If poll returns timeout (no message): continue, don't process
  - _Requirements: 3.1, 3.2, 12.1, 12.2_

- [x]\* 12.1 Write unit tests for consumer lifecycle
  - Test consumer joins group, fetches from partition
  - Test SIGTERM: signal sent, current job processing completes, consumer closes, exit
  - Test offset committed only after successful process
  - Test crash without commit: offset not advanced, message re-delivered on rebalance

- [x] 13. Worker Pod: S3 source download and local staging
  - Create function: `downloadSourceFromS3(ctx, jobID, s3URI) → /tmp/{jobID}/original.mp4`
  - Use S3 SDK, stream to disk (limit temp space to available disk)
  - Handle network errors: retry with backoff
  - Handle not found (404) → permanent failure, return error
  - Log download size and time
  - _Requirements: 3.2_

- [x]\* 13.1 Write unit tests for S3 download
  - Mock S3, test successful download to temp file
  - Test network error → retry and succeed
  - Test 404 → permanent error (no retry)
  - Test out of disk space → return ResourceConstraintError

- [x] 14. Worker Pod: ffmpeg invocation for single rendition
  - Create function: `transcodeSingleRendition(ctx, sourceFile, rendition) → outputFile`
  - Build ffmpeg command: `-i input -c:v codec -b:v bitrate -s resolution ...`
  - Execute with timeout (30 min default), capture stdout/stderr
  - If exitcode != 0: return TranscodingError with stderr
  - Extract duration from ffmpeg output (Duration: HH:MM:SS)
  - Return metadata: rendition_id, file_path, file_size, duration_seconds
  - _Requirements: 3.3, 3.5_

- [x]\* 14.1 Write unit tests for ffmpeg transcoding
  - Mock ffmpeg (shell wrapper script), test command building
  - Test valid rendition → output file created
  - Test invalid codec → ffmpeg exits 1, error captured
  - Test timeout exceeded → process killed, error returned

- [x] 15. Worker Pod: HLS segment generation
  - Create function: `transcodeHLS(ctx, sourceFile, rendition) → hls_dir`
  - Build ffmpeg command with HLS output flags: `-f hls -hls_time 6 playlist.m3u8`
  - Generate segments: segment-00000.ts, segment-00001.ts, ... playlist.m3u8
  - Verify playlist generated and segments exist
  - Return metadata: rendition_id, playlist_path, segment_count
  - _Requirements: 4.3_

- [x]\* 15.1 Write unit tests for HLS generation
  - Mock ffmpeg with script that creates dummy segments
  - Test HLS command built correctly
  - Verify playlist.m3u8 and .ts files created
  - Test segment count matches expected

- [x] 16. Worker Pod: Manifest generation
  - Create function: `generateManifest(ctx, job, results) → manifest.json`
  - Build JSON: job_id, source_file, output_files array (with paths, sizes, durations), generation_time, worker_pod_id (from HOSTNAME env), ffmpeg_version
  - Verify JSON valid, write to file: `{temp_dir}/manifest.json`
  - _Requirements: 4.4_

- [x]\* 16.1 Write property test for manifest schema
  - **Property 6: Manifest Generation Schema**
  - **Validates: Requirements 4.4**
  - Generate random result sets (0-5 renditions), generate manifests
  - Verify valid JSON, all required fields present, generation_time ISO 8601

- [x] 17. Worker Pod: S3 output upload
  - Create function: `uploadOutputsToS3(ctx, jobID, results, manifestPath)`
  - Upload each file in results: `s3://pulsegrid-output/{jobID}/{rendition}/{filename}`
  - Upload manifest to: `s3://pulsegrid-output/{jobID}/manifest.json`
  - Tag all files: job_id, completion_time, rendition
  - Handle transient S3 errors: retry with exponential backoff
  - Handle permanent errors (403): return error, don't retry
  - _Requirements: 3.4, 7.1, 7.2_

- [x]\* 17.1 Write unit tests for S3 upload
  - Mock S3, test successful upload with tags
  - Test transient error (503) → retry → success
  - Test permanent error (403) → no retry
  - Test partial failure (1 file fails) → return error, roll back or cleanup

- [x] 18. Worker Pod: Job completion and retry/DLQ handling (with error classification)
  - **Error Classification** (before retry decision):
    - **Retryable** (transient): network timeout, S3 503/SlowDown, Kafka unavailable, temp disk full
    - **Permanent** (non-retryable): corrupted video file, unsupported codec, source file missing (404), invalid S3 path
    - **Resource constraint** (non-retryable, pod-fatal): out of disk, OOM — exit pod immediately
  - On successful transcoding: commit Kafka offset, emit `pulsegrid_job_completed` metric, record status event
  - On retryable error: increment retry_count, if retry_count < 3 → re-enqueue to Kafka with updated retry_count
  - On permanent error OR retry_count >= 3: publish to DLQ topic with: job_id, failure_reason, timestamp, pod_id, retry_count
  - Emit metrics: `pulsegrid_transcode_failure` (counter, labeled error_type: "retryable"|"permanent"|"constraint")
  - _Requirements: 2.4, 2.5, 3.7, 11.1, 11.5_

- [x]\* 18.1 Write property test for retry logic (retryable errors only)
  - **Property 3: Retry Count Increment on Failure**
  - **Validates: Requirements 2.4**
  - Generate jobs with retry_count in [0, 2], mock retryable error, verify re-enqueue with +1
- [x]\* 18.2 Write property test for DLQ entry (permanent errors)
  - **Property 4: Dead Letter Queue Entry on Max Retries OR Permanent Errors**
  - **Validates: Requirements 2.5, 11.1, 11.5**
  - Generate jobs with retry_count = 3, mock permanent error, verify DLQ message
  - Generate jobs with retry_count = 0, mock permanent error (e.g., unsupported codec), verify immediate DLQ (no retry)

- [x] 19. Worker Pod: Temporary file cleanup
  - After job completes (success or failure): delete `/tmp/{jobID}` directory recursively
  - Log deletion result, handle permission errors gracefully
  - _Requirements: 3.6_

- [x]\* 19.1 Write property test for temp file cleanup
  - **Property 9: Temporary File Cleanup**
  - **Validates: Requirements 3.6**
  - Create temp files in /tmp/{jobID}, process job, verify directory removed
  - Test both success and failure paths

- [x] 20. Worker Pod: Error logging with context
  - Implement structured JSON logging (zap or similar)
  - All error logs include: timestamp (ISO 8601), job_id, pod_id (HOSTNAME), error_message, event_type
  - Log failures with: ffmpeg_stderr (first 500 chars), retry_count, error_type
  - _Requirements: 3.5, 12.2_

- [x]\* 20.1 Write property test for error logging
  - **Property 10: Error Logging Context**
  - **Validates: Requirements 3.5, 12.2**
  - Simulate errors, capture logs, verify all required fields in output
  - Parse structured JSON logs

- [x] 21. Worker Pod: Prometheus metrics emission
  - Emit: `pulsegrid_transcode_duration_seconds` (histogram, labeled by rendition), `pulsegrid_transcode_failures_total` (counter, labeled by error_type), `pulsegrid_job_completed_total` (counter)
  - Emit pod metrics: `pulsegrid_pod_resource_constrained` on ResourceConstraintError
  - Expose /metrics endpoint on port 8081 (Prometheus format)
  - _Requirements: 8.3_

- [x] 22. Checkpoint - Worker Pod functional tests
  - End-to-end test: consume Kafka message → download source → transcode → upload → emit metrics → commit
  - Test all renditions produced correctly
  - Test S3 objects tagged and in correct paths
  - Test metrics emitted with correct labels
  - Test manifest valid JSON and contains all files
  - Ask user if questions arise.

- [x] 23. Job Queue Contract: Kafka producer and consumer abstraction
  - Create pkg/queue/kafka.go with interface: `MessageQueue` (Publish, Consume, Commit, SendDLQ)
  - Implement with Kafka client
  - Encapsulate retry logic, partition assignment, consumer group
  - Allow easy mocking for tests
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [x]\* 23.1 Write property test for queue schema
  - **Property 2: Kafka Job Message Schema Compliance** (integrated test)
  - **Validates: Requirements 2.1**
  - Publish various job specs, consume, verify schema consistent

- [x] 24. Retry Logic: Exponential backoff utility
  - Implemented in `pkg/retry.go` as `RetryWithBackoff(ctx, maxAttempts, baseDelay, fn)`
  - Formula: baseDelay \* 2^attempt, capped at 16s
  - Used by S3 upload/download, Kafka publish, DB connection
  - Tests in `pkg/retry_test.go` (success, retry, all-fail, context cancel, exponential verify, cap verify)
  - _Requirements: 1.6 (implicit), 11 (implicit)_

- [x] 25. Database Migrations and Schema Setup
  - ~~Create migrations: `db/migrations/001_create_jobs_table.sql`, `002_create_job_status_events.sql`~~ ✓
  - ~~Run migrations on startup (using migrate library)~~ ✓
  - ~~Create indexes: `jobs(status)`, `jobs(submission_time)`, `job_status_events(job_id, event_timestamp)`~~ ✓
  - ~~Create TimescaleDB hypertable for job_status_events~~ ✓
  - _Requirements: 5.1, 5.2_

- [x]\* 25.1 Write unit tests for schema
  - Test migrations run without errors
  - Test tables created with correct columns and types
  - Test indexes exist
  - Query test: insert job, query by id, verify result

- [x] 26. All Requirements Mapping and Validation
  - Verify each requirement has at least one task or test validating it
  - Ensure no hanging code: all components integrated
  - Check all error paths handled
  - _Requirements: All 1-18_

- [x] 27. Checkpoint - Full integration test
  - Mock Kafka, S3, Postgres
  - API: POST /videos/upload → consume worker message → download → transcode → upload → query status
  - Verify end-to-end: job submitted → queued → processed → completed
  - Verify all metrics emitted, logs structured, outputs in correct S3 paths
  - Ask user if questions arise.

- [x] 28. Build configuration and Docker images
  - Create Dockerfile.api: Go binary, ffmpeg not needed, expose 8080/8081
  - Create Dockerfile.worker: Go binary, ffmpeg installed, expose 8081
  - Create .dockerignore, build with multi-stage
  - Create Makefile: targets for build, test, docker-build, docker-push
  - Build scripts for CI/CD
  - _Requirements: 13 (implicit)_
- [x] 29. Kubernetes manifests and RBAC configuration
  - Create kube/api-deployment.yaml from design
  - Create kube/worker-deployment.yaml, KEDA ScaledObject
  - Create ServiceAccounts, Roles, RoleBindings
  - Create ConfigMap, Secrets templates
  - Verify resource requests/limits, probes, environment variables
  - _Requirements: 6, 12, 13_

- [x] 30. Terraform infrastructure code
  - Create terraform/ with main.tf, variables.tf, outputs.tf
  - Define: EKS cluster, node groups, S3 buckets, RDS Postgres, VPC, security groups
  - Parameterize: region, environment, instance types, min/max replicas
  - Setup remote state in S3 with encryption and locks
  - Minimal footprint: 2 AZs, single NAT, no MSK (Kafka in-cluster), micro RDS for dev
  - _Requirements: 14_

- [x] 31. Grafana dashboard and Prometheus alerts
  - Create dashboard JSON: queue depth gauge, pod count gauge, p50/p95/p99 latency graphs, failure rate, per-rendition breakdown
  - Create alert rules: HighQueueDepth (>100 for 5m), HighFailureRate (>5%), HighP99Latency (>30min), DLQBacklog (>10)
  - Integrate with API /metrics endpoint
  - _Requirements: 9, 8_

- [x] 32. Load Test Harness
  - Create cmd/load-test/ with configuration: num_jobs, video_size, burst_duration, target_renditions
  - Implement job submission loop: POST /videos/upload, collect job_ids
  - Implement polling loop: GET /jobs/{id}, record submission/completion times
  - Generate output report (JSON): total_jobs, succeeded, failed, latencies (p50/p95/p99), scaling events
  - Validate SLOs: latency targets, success rate, scale-up/down times
  - Generate markdown summary with pass/fail
  - _Requirements: 10_

- [x]\* 32.1 Write unit tests for load test harness
  - Test config parsing
  - Test job submission request building
  - Test status polling and timestamp recording
  - Test report generation and SLO validation

- [x] 33. Integration and E2E validation (environment-realistic substitute — see CHANGELOG "Task 33, 34": no AWS account/live cluster available; real staging deploy/100-job load test/chaos test remain outstanding, listed there under "Not done")
  - Deploy all components to staging cluster
  - Run smoke tests: create job, verify processing, verify completion
  - Run load test with 100 jobs, verify scaling and latency SLOs
  - Run chaos test: kill pod, verify job re-queued and completed
  - Collect metrics, verify no data loss
  - _Requirements: 13, 16, 17_

- [x] 34. Final checkpoint - Production readiness (see CHANGELOG "Task 33, 34" — full local test suite green including real-Postgres integration tests, found and fixed a real TimescaleDB/RDS incompatibility in migration 002, Terraform/Kubernetes manifests statically validated, no secrets found)
  - Verify all tests passing (unit, integration, property, e2e)
  - Verify all requirements covered and validated
  - Verify metrics, logging, error handling comprehensive
  - Verify documentation complete (README, architecture, API docs)
  - Verify no secrets in code or configs
  - Ask user if questions arise

## Notes

- **CRITICAL FIX 1 — Kafka Semantics (Task 12)**: Original design doc used SQS terminology ("message locked", "visibility timeout"). Kafka has no lock mechanism. Recovery on crash = consumer group rebalance + uncommitted offset reprocessed by next consumer. Session timeout (not visibility timeout) keeps consumer alive during long jobs. Task 12 corrected to real Kafka semantics.

- **CRITICAL FIX 2 — DB-Kafka Write Order (Task 6)**: Prevent orphans (job in queue but no DB record). Write DB first with status='submitting', then Kafka, then update DB to status='submitted'. If Kafka fails, rollback DB. If DB fails after Kafka, log alert — operator intervention needed. Task 6 now explicit.

- **CRITICAL FIX 3 — Error Classification (Task 18)**: Req 11.5 says non-retryable failures skip retry, go straight to DLQ. Task 18 now branches: retryable (transient) → retry if count < 3; permanent (bad codec, corrupt file) → immediate DLQ; constraint (OOM) → exit pod. Tests added for permanent-error-immediate-DLQ case.

- Queue depth gauge (task 9.2) deferred to wave 5 after task 12 (worker consumer exists), or query Kafka admin API directly (no dependency on worker lag).

- All property tests (marked with \*) use 100+ iterations of generated inputs

- Core component tests unit tested; external services (S3, Kafka, Postgres) mocked in unit tests, real in integration

- Retry logic consistent across S3, Kafka, DB (exponential backoff, max 5 attempts)

- Error handling: transient → retry; permanent → fail/DLQ; resource constraint → exit pod

- Metrics labeled correctly: job_id (sensitive), rendition (breakdown), error_type (failure analysis)

- All logs structured JSON with required context: timestamp, job_id, pod_id, event_type, message

- Temp cleanup happens on all paths (success and failure)

- Manifests validated as JSON before upload

- No hanging code: all components wired, all outputs integrated

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "1.1"] },
    { "id": 1, "tasks": ["2", "2.1", "3", "3.1"] },
    { "id": 2, "tasks": ["4", "4.1", "5", "5.1"] },
    { "id": 3, "tasks": ["6", "6.1", "7", "7.1", "8", "9", "9.1"] },
    { "id": 4, "tasks": ["10", "12", "12.1"] },
    { "id": 5, "tasks": ["9.2", "13", "13.1", "14", "14.1", "15", "15.1"] },
    { "id": 6, "tasks": ["16", "16.1", "17", "17.1"] },
    {
      "id": 7,
      "tasks": ["18", "18.1", "18.2", "19", "19.1", "20", "20.1", "21"]
    },
    { "id": 8, "tasks": ["23", "23.1", "24", "25", "25.1"] },
    { "id": 9, "tasks": ["26", "27", "28", "29", "30", "31", "32", "32.1"] },
    { "id": 10, "tasks": ["33", "34"] }
  ]
}
```

---

## Wave 11 — Analytics Pipeline Extension

> **Dependency**: All Wave 10 tasks complete (worker pod fully operational). Worker must exist before lifecycle event publishing can be instrumented.

- [x] 35. Worker Pod: Publish job lifecycle events to `job-lifecycle-events` topic (see CHANGELOG "Task 35, 35.1, 36")
  - Add Kafka producer to worker (reuse existing producer abstraction from `pkg/queue/kafka.go`)
  - Define `JobLifecycleEvent` struct: job_id, event_type, rendition_id (nullable), error_class (nullable), error_reason (nullable), pod_id (HOSTNAME), timestamp (ISO 8601)
  - Emit events at: job start (event_type=job_started), each rendition completion (rendition_completed), job completion (job_completed), job failure (job_failed)
  - Partition by job_id hash (same as transcoding-jobs topic) for ordered per-job event delivery
  - Publish is fire-and-forget with retry (do NOT block job processing if analytics publish fails — log and continue)
  - Add env var: `LIFECYCLE_TOPIC=job-lifecycle-events`
  - _Requirements: 19.1, 19.2_

- [x]* 35.1 Write property test for lifecycle event schema
  - **Property 11: Job Lifecycle Event Schema**
  - **Validates: Requirements 19.1**
  - Generate random job outcomes (success, failure with varied error_class, partial rendition completion)
  - Publish lifecycle events, consume from `job-lifecycle-events`, verify JSON schema
  - Verify: event_type is one of the four valid values, pod_id non-empty, timestamp valid ISO 8601
  - Verify: rendition_id present for rendition_completed events, null for job_started/completed/failed
  - Verify: error_class present for job_failed, null for all others
  - Run 100+ iterations

- [x] 36. Analytics Consumer: New service scaffold and Kafka consumer (see CHANGELOG "Task 35, 35.1, 36")
  - Create `cmd/analytics-consumer/main.go`
  - Initialize Kafka consumer: topic `job-lifecycle-events`, group `pulsegrid-analytics`
  - Polling loop identical in structure to worker (Task 12): poll → process → commit offset only on successful sink write
  - On sink write failure: do NOT commit offset — event will be reprocessed on restart
  - Handle SIGTERM: drain in-flight events, close consumer cleanly
  - Add env vars: `ANALYTICS_KAFKA_BROKERS`, `ANALYTICS_CONSUMER_GROUP=pulsegrid-analytics`, `LIFECYCLE_TOPIC=job-lifecycle-events`, `ANALYTICS_DB_DSN`
  - _Requirements: 19.3, 19.4, 19.9_

- [x] 37. Analytics Consumer: Postgres sink — schema and upsert logic
  - Create migration: `db/migrations/003_create_analytics_schema.sql`
  - Create schema `analytics` (isolated from `public`)
  - Create `analytics.job_lifecycle_events` table:
    ```sql
    CREATE TABLE analytics.job_lifecycle_events (
      id           BIGSERIAL PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   VARCHAR(30) NOT NULL,
      rendition_id VARCHAR(20),
      error_class  VARCHAR(20),
      error_reason TEXT,
      pod_id       VARCHAR(255) NOT NULL,
      event_time   TIMESTAMPTZ NOT NULL,
      received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX idx_ale_job_id      ON analytics.job_lifecycle_events (job_id);
    CREATE INDEX idx_ale_event_time  ON analytics.job_lifecycle_events (event_time DESC);
    CREATE INDEX idx_ale_event_type  ON analytics.job_lifecycle_events (event_type);
    ```
  - Convert to TimescaleDB hypertable on `event_time` (consistent with operational schema pattern)
  - Implement `sinkEvent(ctx, event)`: INSERT into `analytics.job_lifecycle_events` (append-only — no upsert needed since events are immutable facts)
  - _Requirements: 19.4, 19.5_

- [x]* 37.1 Write unit tests for analytics sink
  - Mock Postgres, test successful INSERT
  - Test that failed INSERT returns error (consumer will not commit offset)
  - Test schema isolation: INSERT does not touch `public` schema tables
  - Test received_at is set to server time, not event_time

- [x] 38. Analytics Consumer: SQL aggregate views
  - Create migration: `db/migrations/004_create_analytics_views.sql`
  - Implement four views:
    ```sql
    -- v_throughput_per_minute
    CREATE MATERIALIZED VIEW analytics.v_throughput_per_minute AS
    SELECT
      date_trunc('minute', event_time) AS minute,
      COUNT(*) AS jobs_completed
    FROM analytics.job_lifecycle_events
    WHERE event_type = 'job_completed'
      AND event_time > NOW() - INTERVAL '24 hours'
    GROUP BY 1 ORDER BY 1 DESC;

    -- v_latency_percentiles (requires pairing job_started + job_completed per job)
    CREATE MATERIALIZED VIEW analytics.v_latency_percentiles AS
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

    -- v_failure_rate_by_class
    CREATE MATERIALIZED VIEW analytics.v_failure_rate_by_class AS
    SELECT
      date_trunc('hour', event_time) AS hour,
      error_class,
      COUNT(*) AS failure_count,
      ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY date_trunc('hour', event_time)), 0), 2) AS failure_rate_pct
    FROM analytics.job_lifecycle_events
    WHERE event_type = 'job_failed'
      AND event_time > NOW() - INTERVAL '24 hours'
    GROUP BY 1, 2 ORDER BY 1 DESC, 2;

    -- v_rendition_breakdown
    CREATE MATERIALIZED VIEW analytics.v_rendition_breakdown AS
    SELECT
      rendition_id,
      COUNT(*) FILTER (WHERE event_type = 'rendition_completed') AS completed_count,
      COUNT(*) FILTER (WHERE event_type = 'job_failed') AS failed_count,
      AVG(EXTRACT(EPOCH FROM (c.event_time - s.event_time))) AS avg_duration_seconds
    FROM analytics.job_lifecycle_events c
    JOIN analytics.job_lifecycle_events s
      ON c.job_id = s.job_id
     AND c.event_type = 'rendition_completed'
     AND s.event_type = 'job_started'
    WHERE c.event_time > NOW() - INTERVAL '24 hours'
    GROUP BY 1;
    ```
  - Add refresh logic: `REFRESH MATERIALIZED VIEW CONCURRENTLY` called every 60 seconds from Analytics_Consumer's background goroutine
  - _Requirements: 19.6_

- [x]* 38.1 Write unit tests for view SQL logic
  - Insert controlled test data into `analytics.job_lifecycle_events`
  - REFRESH views, query each view
  - Test v_throughput_per_minute: 10 completed events in 10 minutes → 10 rows, 1 each
  - Test v_latency_percentiles: known durations → verify p50 within 5% of expected
  - Test v_failure_rate_by_class: 3 retryable, 1 permanent → verify rates sum to 100%
  - Test v_rendition_breakdown: 5 720p completions, 2 failures → verify counts correct

- [x] 39. API Server: GET /analytics/summary endpoint
  - Add route: `GET /analytics/summary`
  - Query all four materialized views in parallel (4 goroutines, context with 5s timeout)
  - Return JSON:
    ```json
    {
      "generated_at": "2024-01-15T10:30:00Z",
      "throughput_per_minute": [...],
      "latency_percentiles": [...],
      "failure_rate_by_class": [...],
      "rendition_breakdown": [...]
    }
    ```
  - Return 200 on success, 503 if any view query times out (include partial data)
  - Read-only: no writes from this endpoint
  - _Requirements: 19.7_

- [x]* 39.1 Write unit tests for /analytics/summary
  - Mock analytics DB, test all four sub-queries called in parallel
  - Test 200 with full payload on success
  - Test 503 with partial payload when one view times out
  - Test that endpoint does not touch `public` schema tables

- [x] 40. Analytics Consumer: Prometheus metrics endpoint
  - Expose `/metrics` on port 8082
  - Emit: `pulsegrid_analytics_events_processed_total` (counter, labeled by event_type)
  - Emit: `pulsegrid_analytics_sink_lag_seconds` (gauge: `received_at - event_time` latency)
  - Emit: `pulsegrid_analytics_consumer_errors_total` (counter, labeled by error_type: sink_write_failure | parse_error | kafka_poll_error)
  - _Requirements: 19.10_

- [x]* 40.1 Write unit tests for analytics consumer metrics
  - Simulate 10 events of varied types, verify counter increments match
  - Simulate sink failure, verify error counter incremented and offset NOT committed
  - Verify lag gauge value is within 1 second of actual delta on controlled test

- [x] 41. Grafana: Analytics dashboard row
  - Add "Analytics" row to existing Grafana dashboard JSON (`monitoring/grafana/dashboard.json`)
  - Panel 1: Throughput trend — line chart from `v_throughput_per_minute`, last 24h
  - Panel 2: Latency percentiles — bar chart (p50/p95/p99) from `v_latency_percentiles`, last 7d by hour
  - Panel 3: Failure rate by class — stacked bar from `v_failure_rate_by_class`, last 24h
  - Panel 4: Rendition breakdown — table from `v_rendition_breakdown`, last 24h
  - All panels source from `analytics` schema via existing Postgres datasource
  - _Requirements: 19.8_

- [x] 42. Kubernetes: Analytics Consumer deployment manifest
  - Create `kube/analytics-consumer-deployment.yaml`
  - Single replica (no autoscaling needed — consumer group rebalance handles failover)
  - Resource requests: 100m CPU, 128Mi RAM (lightweight sink, not compute-intensive)
  - Expose port 8082 for Prometheus scrape
  - Add liveness probe: `GET /health` (implement lightweight health handler — checks Kafka connectivity + DB connectivity)
  - Add env vars mirroring worker pattern: `ANALYTICS_KAFKA_BROKERS`, `LIFECYCLE_TOPIC`, `ANALYTICS_DB_DSN`
  - _Requirements: 19.3, 19.9_

- [x] 43. Checkpoint — Analytics pipeline end-to-end (see CHANGELOG "Task 39-43" — environment-realistic substitute, no live EKS/Grafana available; real-Postgres integration test chains the whole pipeline instead)
  - Deploy Analytics_Consumer to staging cluster alongside existing components
  - Submit 20 test jobs via load test harness
  - Verify `job-lifecycle-events` topic receives events for all jobs (kafka-console-consumer check)
  - Verify `analytics.job_lifecycle_events` table rows match event count
  - REFRESH all materialized views, query each, verify non-empty results
  - Hit `GET /analytics/summary`, verify 200 and all four sections populated
  - Open Grafana Analytics row, verify all four panels render with data
  - Verify Analytics_Consumer `/metrics` endpoint shows correct event counts and lag < 5s
  - _Requirements: 19.1–19.10_

## Task Dependency Graph (updated)

```json
{
  "waves": [
    { "id": 0,  "tasks": ["1", "1.1"] },
    { "id": 1,  "tasks": ["2", "2.1", "3", "3.1"] },
    { "id": 2,  "tasks": ["4", "4.1", "5", "5.1"] },
    { "id": 3,  "tasks": ["6", "6.1", "7", "7.1", "8", "9", "9.1"] },
    { "id": 4,  "tasks": ["10", "12", "12.1"] },
    { "id": 5,  "tasks": ["9.2", "13", "13.1", "14", "14.1", "15", "15.1"] },
    { "id": 6,  "tasks": ["16", "16.1", "17", "17.1"] },
    { "id": 7,  "tasks": ["18", "18.1", "18.2", "19", "19.1", "20", "20.1", "21"] },
    { "id": 8,  "tasks": ["23", "23.1", "24", "25", "25.1"] },
    { "id": 9,  "tasks": ["26", "27", "28", "29", "30", "31", "32", "32.1"] },
    { "id": 10, "tasks": ["33", "34"] },
    { "id": 11, "tasks": ["35", "35.1", "36", "37", "37.1", "38", "38.1", "39", "39.1", "40", "40.1", "41", "42", "43"] }
  ]
}
```

## Notes (additions)

- **Analytics consumer is a separate binary** (`cmd/analytics-consumer`), not folded into the worker or API server. This keeps failure domains isolated — a sink outage does not affect job processing throughput.
- **Fire-and-forget publish from worker** (Task 35): lifecycle event publish errors are logged and skipped, never blocking the transcoding critical path. The analytics pipeline is best-effort from the worker's perspective; at-least-once delivery is enforced on the consumer side via offset commit gating.
- **Materialized views over live queries**: the four views are materialized (not regular views) because the analytics queries involve self-joins and percentile aggregations that are expensive on a live table. CONCURRENT refresh every 60s is acceptable given the dashboard's polling interval.
- **Schema isolation** (`analytics` vs `public`): prevents analytics aggregation queries from taking table-level locks or causing autovacuum pressure on the operational `jobs` table.
- **Property 11** follows the same 100-iteration convention as all other property tests.