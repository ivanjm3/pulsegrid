# Pulsegrid

Distributed video transcoding platform. Accepts uploads via REST API, enqueues jobs to Kafka, processes with horizontally-scalable worker pods (ffmpeg), outputs to S3. KEDA-based autoscaling on queue depth.

## Project Structure

```
pulsegrid/
├── cmd/
│   ├── api/         # API server entrypoint
│   └── worker/      # Worker pod entrypoint
├── pkg/             # Shared types, errors, utilities
├── docs/            # Architecture, API docs, interview notes
└── go.mod
```

## Core Types

- `Job` — full transcoding job with source, renditions, status, timestamps
- `Rendition` — target output format (resolution, codec, bitrate, HLS config)
- `JobStatus` — enum: submitted, processing, completed, failed
- `RetryConfig` — max retries, base delay, max delay

## Error Types

- `TranscodingError` — ffmpeg failure (retryable up to max retries)
- `ResourceConstraintError` — OOM/disk full (non-retryable, pod exits)

## Job ID Generation

UUID v4 via `crypto/rand`. RFC 4122 compliant. Property-tested for uniqueness + format.

## API Server

HTTP server on port 8080. Accepts video uploads via `POST /videos/upload` (multipart/form-data).

- File size limit: 10GB (enforced via `MaxBytesReader`)
- Validates: video file (required), source_name (required), renditions JSON (optional)
- Returns HTTP 202 with job_id, status_uri, submission_time
- Structured error responses with request_id for tracing

See [docs/api.md](docs/api.md) for full endpoint documentation.

## Build

```bash
go build ./...
go test ./...
```

## Run API Server

```bash
go run ./cmd/api/
# Listening on :8080
```
