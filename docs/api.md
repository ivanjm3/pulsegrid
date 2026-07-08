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
| `handleVideoUpload` | Main handler — orchestrates parse/validate/upload/atomic-enqueue/respond |
| `handleListJobs` | GET /jobs handler — parses filters, queries DB, returns paginated results |
| `parseRenditions` | JSON parse + schema validation for renditions |
| `defaultRenditions` | Returns standard 3-rendition set |
| `writeError` | Structured error response writer |
| `writeErrorWithRequestID` | Error response with known request ID |
| `pkg.NewS3Client` | Creates S3Client with multipart upload manager |
| `S3Client.UploadSourceToS3` | Streams file to S3 with tagging + retry |
| `pkg.NewKafkaClient` | Creates KafkaClient with writers for main + DLQ topics |
| `KafkaClient.EnqueueJob` | Serializes Job to JSON, publishes to Kafka with retry |
| `KafkaClient.SendDLQ` | Publishes failed job to dead-letter queue |
| `pkg.RetryWithBackoff` | Generic exponential backoff retry utility |
| `pkg.NewPostgresClient` | Creates pgxpool connection with retry on initial connect |
| `PostgresClient.RecordJobMetadata` | INSERT job into jobs table |
| `PostgresClient.RecordStatusEvent` | INSERT event into job_status_events table |
| `PostgresClient.GetJobByID` | SELECT job by job_id |
| `PostgresClient.QueryJobs` | SELECT jobs with filters (time range, status, pagination) |
| `PostgresClient.InsertJobTx` | BEGIN TX + INSERT job with status='submitting', returns TxHandle |
| `TxHandle.UpdateStatusAndCommit` | UPDATE status + COMMIT within transaction |
| `TxHandle.Rollback` | ROLLBACK transaction (safe if already committed) |

## S3 Integration

- **Bucket**: `pulsegrid-source` (configurable via `PULSEGRID_SOURCE_BUCKET` env var)
- **Key pattern**: `{jobID}/original.mp4`
- **Upload method**: AWS SDK v2 `s3/manager.Uploader` (multipart, 10MB parts, 5 concurrent)
- **Object tags**: `job_id`, `upload_time` (ISO 8601), `source_name`
- **Retry**: Exponential backoff (1s, 2s, 4s, 8s, 16s) — max 5 attempts
- **Interface**: `S3Uploader` interface allows nil/mock for local dev/testing
- **Local dev**: If `AWS_REGION` not set, S3 upload skipped (synthetic URI returned)

## Kafka Integration

- **Library**: `github.com/segmentio/kafka-go`
- **Topic**: `transcoding-jobs` (configurable via `KAFKA_TOPIC`)
- **DLQ Topic**: `transcoding-dlq` (configurable via `KAFKA_DLQ_TOPIC`)
- **Partitioning**: Hash of job_id as message key (kafka-go Hash balancer)
- **Acks**: RequireAll (waits for all ISR to acknowledge)
- **Retry**: Exponential backoff (1s, 2s, 4s, 8s, 16s) — max 5 attempts
- **Interface**: `KafkaProducer` interface allows nil/mock for local dev/testing
- **Local dev**: If `KAFKA_BROKERS` not set, enqueue skipped

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
| `DATABASE_URL` | No | — | Postgres connection string (enables DB when set) |

## Design Decisions

- **Atomic DB-Kafka write ordering**: DB first (status='submitting') → Kafka → DB update (status='submitted') → commit. Prevents orphans (job in queue but no DB record). If Kafka fails, rollback makes job invisible. If commit fails after Kafka, log ALERT for operator.
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
