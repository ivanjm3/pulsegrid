# Pulsegrid API Documentation

## Server

- **Port**: 8080
- **Framework**: Go stdlib `net/http`
- **No external dependencies**

## Endpoints

### POST /videos/upload

Accepts multipart/form-data video uploads. Returns job tracking info.

**Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| video | file | yes | Video file (max 10GB) |
| source_name | string | yes | Human-readable name for tracking |
| renditions | JSON string | no | Array of rendition specs (defaults applied if omitted) |

**Default Renditions** (when field omitted):
```json
[
  {"id":"720p","resolution":"1280x720","video_codec":"libx264","video_bitrate":"5M","audio_codec":"aac","audio_bitrate":"128k"},
  {"id":"480p","resolution":"854x480","video_codec":"libx264","video_bitrate":"2.5M","audio_codec":"aac","audio_bitrate":"96k"},
  {"id":"hls","type":"hls_segments","segment_duration":6,"base_resolution":"720p"}
]
```

**Rendition Validation**: Each rendition must have at least `id` or `type` field set.

**Success Response** (HTTP 202):
```json
{
  "job_id": "uuid-v4",
  "status_uri": "/jobs/{job_id}",
  "estimated_wait_time_seconds": 120,
  "submission_time": "2024-01-15T10:30:00Z"
}
```

**Error Responses:**

| Status | Condition | error_code |
|--------|-----------|------------|
| 400 | Missing field, invalid JSON, bad renditions | VALIDATION_ERROR |
| 413 | File exceeds 10GB | PAYLOAD_TOO_LARGE |
| 500 | Internal failure | INTERNAL_ERROR |

**Error Format:**
```json
{
  "error": "Human-readable message",
  "error_code": "VALIDATION_ERROR",
  "request_id": "uuid-for-tracing",
  "timestamp": "2024-01-15T10:30:00Z",
  "detail": "Additional context"
}
```

---

### GET /jobs

Query jobs with optional time range, status, and pagination filters.

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| submitted_after | ISO 8601 string | no | — | Inclusive lower bound on submission_time |
| submitted_before | ISO 8601 string | no | — | Inclusive upper bound on submission_time |
| status | comma-separated string | no | — | Filter by status (submitted, processing, completed, failed) |
| limit | integer | no | 100 | Max results per page (max 1000) |
| offset | integer | no | 0 | Pagination offset |

**Validation:**
- `submitted_after` and `submitted_before` must be valid ISO 8601 (RFC 3339) timestamps
- `submitted_after` must be before `submitted_before` if both provided
- `limit` must be 1–1000
- `offset` must be >= 0
- `status` values must be one of: submitted, processing, completed, failed

**Success Response** (HTTP 200):
```json
{
  "jobs": [
    {
      "job_id": "uuid-v4",
      "status": "completed",
      "submission_time": "2024-01-15T10:30:00Z",
      "completion_time": "2024-01-15T10:35:00Z",
      "duration_seconds": 300
    }
  ],
  "total": 5000,
  "limit": 100,
  "offset": 0
}
```

**Error Responses:**

| Status | Condition | error_code |
|--------|-----------|------------|
| 400 | Invalid timestamp, bad status, limit/offset out of range | VALIDATION_ERROR |
| 500 | Database query failure | INTERNAL_ERROR |
| 503 | Database not configured | SERVICE_UNAVAILABLE |

---

### GET /health

Health check endpoint for Kubernetes liveness/readiness probes. Checks connectivity to all backend dependencies.

**Success Response** (HTTP 200 — all healthy):
```json
{
  "status": "healthy",
  "checks": {
    "postgres": {"status": "healthy"},
    "kafka": {"status": "healthy"},
    "s3": {"status": "healthy"}
  }
}
```

**Failure Response** (HTTP 503 — one or more unhealthy):
```json
{
  "status": "unhealthy",
  "checks": {
    "postgres": {"status": "unhealthy", "error": "connection refused"},
    "kafka": {"status": "healthy"},
    "s3": {"status": "healthy"}
  }
}
```

**Check Statuses:**
- `healthy` — component reachable and responding
- `unhealthy` — component unreachable (includes error message)
- `not_configured` — component not configured (local dev mode)

**Behavior:**
- 5-second timeout on all health checks (prevents slow probe from hanging)
- Returns 200 if all configured components are healthy (or none configured)
- Returns 503 if any configured component fails ping
- Postgres: uses `pool.Ping()` (verifies connection pool has live connection)
- Kafka: dials broker TCP connection (verifies network reachability)
- S3: calls `HeadBucket` (verifies bucket exists and credentials valid)

**Kubernetes Usage:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  periodSeconds: 15
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /health
    port: 8080
  periodSeconds: 10
  failureThreshold: 2
```

---

### GET /metrics

Exposes Prometheus metrics in exposition format. Scraped by Prometheus at configured interval.

**API Server Metrics (port 8080):**

| Metric | Type | Description |
|--------|------|-------------|
| `pulsegrid_jobs_submitted_total` | counter | Total jobs successfully submitted (incremented on 202 response) |
| `pulsegrid_upload_duration_seconds` | histogram | Duration of full upload flow (request start → 202 response) |
| `pulsegrid_queue_depth_jobs` | gauge | Current number of jobs waiting in transcoding-jobs Kafka topic (updated every 30s) |

**Worker Pod Metrics (port 8081):**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pulsegrid_job_completed_total` | counter | — | Total jobs completed successfully |
| `pulsegrid_transcode_failures_total` | counter | `error_type` (retryable\|permanent\|constraint) | Total transcode failures by category |
| `pulsegrid_transcode_duration_seconds` | histogram | `rendition` | Transcode duration per rendition |
| `pulsegrid_pod_resource_constrained` | counter | — | Total times pod exited due to resource constraint (OOM, disk full) |

**Histogram Buckets**: 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0 seconds

**Queue Depth Gauge:**
- Polls Kafka admin API every 30s via background goroutine
- Calculates: sum(end_offset - committed_offset) across all partitions of `transcoding-jobs` topic
- Uses consumer group `pulsegrid-workers` (configurable via `KAFKA_CONSUMER_GROUP`)
- Returns 0 if no consumer group committed offsets exist (all messages unconsumed)
- Gracefully handles broker unavailability (logs warning, keeps last value)

**Notes:**
- Metrics only emitted on successful submission (not on 4xx/5xx errors)
- Uses injectable `*pkg.Metrics` struct — test isolation via custom `prometheus.Registry`

---

## Architecture

```
Client → POST /videos/upload (multipart/form-data)
       → MaxBytesReader enforces 10GB limit
       → ParseMultipartForm (32MB memory buffer)
       → Validate source_name (required)
       → Validate video file (required)
       → Parse + validate renditions JSON
       → Generate UUID v4 Job ID
       → Stream upload to S3: s3://pulsegrid-source/{jobID}/original.mp4
         (multipart upload, no local disk buffering)
         Tags: job_id, upload_time (ISO 8601), source_name
         Retry: exponential backoff 1s/2s/4s/8s/16s, max 5 attempts
       → ATOMIC DB-KAFKA ORDERING (prevents orphans):
         1. BEGIN TX: INSERT job with status='submitting'
         2. Publish to Kafka (topic "transcoding-jobs", key=job_id)
            - If Kafka fails: ROLLBACK TX → return 500 (job never existed)
         3. UPDATE job status='submitted', COMMIT TX
            - If commit fails: log ALERT (orphan in queue) → still return 202
       → Record status event (best-effort, non-fatal)
       → Return HTTP 202 with job tracking info
```

## Key Functions

| Function | Purpose |
|----------|---------|
| `handleVideoUpload` | Main handler — orchestrates parse/validate/upload/atomic-enqueue/metrics-emit/respond |
| `handleListJobs` | GET /jobs handler — parses filters, queries DB, returns paginated results |
| `handleGetJob` | GET /jobs/{job_id} handler — fetches job by ID, returns status/outputs |
| `handleHealth` | GET /health handler — pings Postgres, Kafka, S3 and returns aggregate health |
| `parseRenditions` | JSON parse + schema validation for renditions |
| `defaultRenditions` | Returns standard 3-rendition set |
| `writeError` | Structured error response writer |
| `writeErrorWithRequestID` | Error response with known request ID |
| `pkg.NewS3Client` | Creates S3Client with multipart upload manager |
| `S3Client.UploadSourceToS3` | Streams file to S3 with tagging + retry |
| `S3Client.DownloadSourceFromS3` | Downloads source video from S3 to local disk with retry; 404 → permanent error |
| `S3Client.UploadOutputsToS3` | Uploads all transcoded outputs (MP4, HLS, manifest) to output bucket with tags + retry |
| `ParseS3URI` | Extracts bucket and key from s3:// URI |
| `TranscodeSingleRendition` | Invokes ffmpeg to produce single MP4 rendition from source |
| `TranscodeHLS` | Invokes ffmpeg with HLS flags to produce playlist.m3u8 + segment-XXXXX.ts files |
| `pkg.NewKafkaClient` | Creates KafkaClient with writers for main + DLQ topics |
| `KafkaClient.EnqueueJob` | Serializes Job to JSON, publishes to Kafka with retry |
| `KafkaClient.SendDLQ` | Publishes failed job to dead-letter queue |
| `pkg.RetryWithBackoff` | Generic exponential backoff retry utility |
| `queue.NewKafkaQueue` | Creates unified MessageQueue (producer+consumer) with retry, partitioning, consumer group |
| `KafkaQueue.Publish` | Publishes KafkaMessage to main topic with retry + Hash partition by job_id |
| `KafkaQueue.Consume` | Fetches and deserializes next message from consumer group |
| `KafkaQueue.Commit` | Commits consumer offset for processed message |
| `KafkaQueue.SendDLQ` | Publishes to DLQ topic with failure metadata + retry |
| `pkg.StartQueueDepthPoller` | Background goroutine polling Kafka for queue depth, updating Prometheus gauge |
| `pkg.NewPostgresClient` | Creates pgxpool connection with retry on initial connect |
| `PostgresClient.RecordJobMetadata` | INSERT job into jobs table |
| `PostgresClient.RecordStatusEvent` | INSERT event into job_status_events table |
| `PostgresClient.GetJobByID` | SELECT job by job_id |
| `PostgresClient.QueryJobs` | SELECT jobs with filters (time range, status, pagination) |
| `PostgresClient.InsertJobTx` | BEGIN TX + INSERT job with status='submitting', returns TxHandle |
| `TxHandle.UpdateStatusAndCommit` | UPDATE status + COMMIT within transaction |
| `TxHandle.Rollback` | ROLLBACK transaction (safe if already committed) |

## S3 Integration

- **Source Bucket**: `pulsegrid-source` (configurable via `PULSEGRID_SOURCE_BUCKET` env var)
- **Output Bucket**: `pulsegrid-output`
- **Source Key pattern**: `{jobID}/original.mp4`
- **Output Key pattern**: `{jobID}/{rendition}/{filename}` (e.g. `{jobID}/720p/720p.mp4`)
- **Manifest Key**: `{jobID}/manifest.json`
- **Upload method**: AWS SDK v2 `s3/manager.Uploader` (multipart, 10MB parts, 5 concurrent)
- **Download method**: AWS SDK v2 `s3.GetObject` → stream to `/tmp/{jobID}/original.mp4`
  - Retry: Exponential backoff (1s, 2s, 4s, 8s, 16s) — max 5 attempts
  - 404 (NoSuchKey): Permanent failure, no retry → `*SourceNotFoundError`
  - Logs download size and elapsed time on success
- **Output Upload**: `UploadOutputsToS3(ctx, jobID, results, hlsResults, manifestPath)`
  - Uploads MP4 renditions, HLS playlist + segments, and manifest.json
  - Tags: `job_id`, `completion_time` (ISO 8601), `rendition`
  - Retry: Exponential backoff (1s, 2s, 4s, 8s, 16s) — max 5 attempts
  - Permanent error (403 AccessDenied): Returns immediately, no retry
  - Transient error (503, network): Retries with backoff
- **Source Object tags**: `job_id`, `upload_time` (ISO 8601), `source_name`
- **Output Object tags**: `job_id`, `completion_time` (ISO 8601), `rendition`
- **Interface**: `S3Uploader` (uploads), `S3Downloader` (downloads), `S3OutputUploader` (output upload) — nil/mock for local dev/testing
- **Local dev**: If `AWS_REGION` not set, S3 upload/download skipped (synthetic URI returned / download skipped)

## Kafka Integration

- **Library**: `github.com/segmentio/kafka-go`
- **Topic**: `transcoding-jobs` (configurable via `KAFKA_TOPIC`)
- **DLQ Topic**: `transcoding-dlq` (configurable via `KAFKA_DLQ_TOPIC`)
- **Partitioning**: Hash of job_id as message key (kafka-go Hash balancer)
- **Acks**: RequireAll (waits for all ISR to acknowledge)
- **Retry**: Exponential backoff (1s, 2s, 4s, 8s, 16s) — max 5 attempts
- **Interface**: `KafkaProducer` interface allows nil/mock for local dev/testing
- **Local dev**: If `KAFKA_BROKERS` not set, enqueue skipped

### Unified Queue Abstraction (pkg/queue)

A higher-level `MessageQueue` interface in `pkg/queue/kafka.go` unifies producer and consumer operations into a single mockable abstraction:

**Interface:**
```go
type MessageQueue interface {
    Publish(ctx context.Context, msg pkg.KafkaMessage) error
    Consume(ctx context.Context) (*Message, error)
    Commit(ctx context.Context, msg *Message) error
    SendDLQ(ctx context.Context, msg pkg.KafkaMessage, reason string) error
    Close() error
}
```

**Key Types:**
- `Message` — wraps `kafka.Message` with deserialized `pkg.KafkaMessage` payload
- `Config` — all queue configuration (brokers, topics, groupID, timeouts, retry params)
- `KafkaQueue` — concrete implementation using `segmentio/kafka-go`

**Configuration (Config struct):**
| Field | Default | Purpose |
|-------|---------|---------|
| SessionTimeout | 30 min | Prevents rebalance during long transcodes |
| HeartbeatInterval | 3s | Consumer group heartbeat frequency |
| MaxWait | 5s | Max poll wait for new messages |
| RetryAttempts | 5 | Publish/DLQ retry attempts |
| RetryBaseDelay | 1s | Exponential backoff base delay |

**Design:**
- Retry logic encapsulated via `pkg.RetryWithBackoff` (exponential backoff, 16s cap)
- Partition assignment via Hash balancer on job_id key
- Consumer group with 30min session timeout for long-running transcodes
- `auto.offset.reset=earliest` (StartOffset: FirstOffset)
- Interface-based — tests use mock implementation without Kafka broker

## Worker Error Classification & Retry/DLQ

**Error Types:**

| Category | Examples | Action |
|----------|----------|--------|
| **Retryable** (transient) | Network timeout, S3 503/SlowDown, Kafka unavailable, temp disk full | Re-enqueue with retry_count+1 if < 3; else DLQ |
| **Permanent** (non-retryable) | Corrupted video, unsupported codec, source 404, invalid S3 path | Send to DLQ immediately |
| **Resource constraint** (pod-fatal) | Out of disk, OOM | Exit pod via `os.Exit(1)` |

**Classification Logic (`pkg.ClassifyError`):**
1. Check typed errors via `errors.As`: `ResourceConstraintError` → constraint, `SourceNotFoundError` → permanent, `PermanentError` → permanent
2. Fallback to string pattern matching on error message (case-insensitive)
3. Default: retryable (safest assumption for unknown errors)

**Retry Flow:**
- Retryable error + retry_count < 3: Increment count, publish updated message to `transcoding-jobs`
- Retryable error + retry_count >= 3: Publish to `transcoding-dlq`
- Permanent error: Publish to `transcoding-dlq` immediately (no retry)
- Resource constraint: `os.Exit(1)` — Kubernetes restarts pod, Kafka rebalances, message redelivered

**DLQ Message Format:**
```json
{
  "job_id": "uuid",
  "source_s3_uri": "s3://...",
  "retry_count": 3,
  "dlq_entry_timestamp": "2024-01-15T10:45:00Z",
  "failure_reason": "ffmpeg: unsupported codec VP9",
  "failure_timestamp": "2024-01-15T10:45:00Z",
  "pod_id": "worker-pod-abc123"
}
```

**Key Functions:**

| Function | Purpose |
|----------|---------|
| `pkg.ClassifyError` | Determines error category (retryable/permanent/constraint) |
| `handleJobOutcome` | Routes job result to success/retry/DLQ/exit path |
| `KafkaClient.ReenqueueWithRetry` | Re-publishes message to main topic with updated retry_count |
| `KafkaClient.SendDLQFromMessage` | Publishes KafkaMessage to DLQ with failure metadata + pod_id |
| `pkg.NewWorkerMetrics` | Registers worker Prometheus metrics (completed, failure, duration) |

## Postgres Integration

- **Library**: `github.com/jackc/pgx/v5/pgxpool`
- **Connection**: `DATABASE_URL` env var (standard Postgres connection string)
- **Pool**: pgxpool manages connection pool automatically (health checks, idle timeout)
- **Retry**: Exponential backoff on initial connection (1s base, 16s cap, 5 attempts)
- **Tables**: `jobs` (metadata), `job_status_events` (TimescaleDB hypertable)
- **Interface**: `DBClient` interface allows nil/mock for local dev/testing
- **Local dev**: If `DATABASE_URL` not set, DB writes skipped
- **Migrations**: `db/migrations/001_create_jobs_table.sql`, `002_create_job_status_events.sql`

**Kafka Message Schema:**
```json
{
  "job_id": "uuid",
  "source_s3_uri": "s3://pulsegrid-source/{job_id}/original.mp4",
  "source_file_size_bytes": 1073741824,
  "renditions": [...],
  "output_s3_prefix": "s3://pulsegrid-output/{job_id}/",
  "retry_count": 0,
  "max_retries": 3,
  "submitted_timestamp": "2024-01-15T10:30:00.000Z",
  "visibility_timeout_seconds": 1800
}
```

## Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `AWS_REGION` | No | — | Enables S3 upload when set |
| `AWS_DEFAULT_REGION` | No | — | Alternative to AWS_REGION |
| `PULSEGRID_SOURCE_BUCKET` | No | `pulsegrid-source` | S3 bucket for source videos |
| `KAFKA_BROKERS` | No | — | Comma-separated broker list (enables Kafka when set) |
| `KAFKA_TOPIC` | No | `transcoding-jobs` | Kafka topic for job messages |
| `KAFKA_DLQ_TOPIC` | No | `transcoding-dlq` | Kafka dead-letter queue topic |
| `KAFKA_CONSUMER_GROUP` | No | `pulsegrid-workers` | Consumer group for queue depth calculation |
| `DATABASE_URL` | No | — | Postgres connection string (enables DB when set) |

## Design Decisions

- **Atomic DB-Kafka write ordering**: DB first (status='submitting') → Kafka → DB update (status='submitted') → commit. Prevents orphans (job in queue but no DB record). If Kafka fails, rollback makes job invisible. If commit fails after Kafka, log ALERT for operator.
- **Prometheus metrics emission**: Counter + histogram emitted only on successful 202 response (not on validation/error paths). Timing starts at handler entry, observed at end. Uses injectable `*Metrics` for test isolation with custom registry.
- **MaxBytesReader** over Content-Length check: CL header can be spoofed; MaxBytesReader enforces at read time
- **Modular handler**: Parse/validate layer is separate — S3/Kafka/DB integration wires in later
- **Structured errors**: Every error has request_id + timestamp for distributed tracing
- **No disk buffering**: Designed for streaming to S3 (wired in later task)
- **Stdlib only**: No gin/chi/echo — keeps binary small, reduces dependency surface
- **S3 behind interface**: `S3Uploader` interface means nil-safe for local dev, mockable for tests
- **Multipart upload manager**: Streams directly from io.Reader → no local disk buffering for 10GB files
- **Exponential backoff**: Generic utility in `pkg/retry.go`, reusable for Kafka/DB later
- **Kafka behind interface**: `KafkaProducer` interface means nil-safe for local dev, mockable for tests
- **Partition by job_id**: Hash balancer uses job_id as key for consistent partition assignment
- **RequireAll acks**: Strongest durability guarantee — message written to all ISR before ack
- **Separate DLQ writer**: Dedicated writer for dead-letter queue with own retry logic
- **Postgres behind interface**: `DBClient` interface — nil-safe for local dev, mockable for tests
- **pgxpool over database/sql**: Native Postgres wire protocol, connection pooling, no ORM overhead
- **TimescaleDB hypertable**: job_status_events is time-series data — hypertable enables efficient range queries
- **Status event best-effort**: Event insert failure doesn't fail the request (job metadata already saved)
- **Three-tier error classification**: retryable (re-enqueue), permanent (DLQ), constraint (pod exit) — enables precise retry semantics
- **Error classification via `errors.As` + string patterns**: typed errors matched first, fallback to message pattern matching for untyped errors
- **Re-enqueue to same topic**: Simpler than Kafka retry topics; retry_count in message body tracks attempts
- **Resource constraint → os.Exit(1)**: Let Kubernetes restart pod; Kafka rebalance redelivers unfinished job to healthy consumer
- **DLQ includes pod_id**: Enables debugging which pod failed and correlating with pod logs/metrics
- **Offset committed after DLQ/re-enqueue**: Prevents double-processing; original message consumed, new message in queue
- **Unified MessageQueue interface**: Single abstraction for both produce and consume — simplifies worker code, enables full pipeline mocking in tests
- **Config with defaults() pattern**: Zero-value fields get production defaults, explicit values preserved — ergonomic for both prod and test usage
