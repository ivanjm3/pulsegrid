# PulseGrid

PulseGrid is a distributed video transcoding platform written in Go. Clients upload a source video over HTTP; the API stores the file in S3, records job state in Postgres, and enqueues the work on Kafka. A horizontally scalable fleet of worker pods consumes jobs, shells out to `ffmpeg` to produce MP4 and HLS renditions, uploads the outputs and a manifest back to S3, and reports status through Postgres and Prometheus. The system is built to survive partial failures (broker outages, OOM'd pods, corrupt sources) without losing or double-processing a job, and ships with the Kubernetes manifests, Terraform, and Grafana/Prometheus assets needed to run it in AWS.

## Features

- **Async video ingestion** — `POST /videos/upload` accepts multipart uploads up to 10GB, streamed directly to S3 with no local disk buffering.
- **Configurable renditions** — caller-supplied or default rendition sets (720p/480p MP4 + adaptive HLS).
- **Exactly-once-ish job bookkeeping** — atomic DB→Kafka→DB write ordering prevents orphaned jobs (a job that exists in the queue but not in Postgres, or vice versa).
- **Three-tier error classification** — retryable, permanent, and resource-constraint failures are handled differently (re-enqueue, DLQ, or pod exit) instead of one generic retry loop.
- **Dead-letter queue** — jobs that exceed retry budget or fail permanently land in `transcoding-dlq` with full failure context and the pod ID that last touched them.
- **Full observability** — Prometheus metrics from both API and worker, a Grafana dashboard, and Prometheus alert rules for queue depth, latency, failure rate, and DLQ backlog.
- **Status query API** — `GET /jobs/{id}` and `GET /jobs` (with time-range, status, and pagination filters) for tracking work in flight.
- **Load-test harness** — a standalone tool (`cmd/load-test`) that submits a burst of jobs and produces an SLO summary.

## Architecture

```
                                   ┌────────────────────┐
                                   │   Client / caller   │
                                   └──────────┬──────────┘
                                              │ POST /videos/upload (multipart)
                                              ▼
┌───────────────────────────────────────────────────────────────────────┐
│  API server (:8080)                                                    │
│  1. MaxBytesReader enforces 10GB cap, validates form fields             │
│  2. Streams source video to S3 (no disk buffering)                     │
│  3. BEGIN TX → INSERT job (status=submitting)                          │
│  4. Publish job to Kafka topic "transcoding-jobs" (key = job_id)       │
│  5. UPDATE job status=submitted → COMMIT TX                            │
│     (Kafka publish failure rolls back the DB insert — no orphan job)   │
│  6. Return 202 with job_id + status URI                                │
└───────────────┬───────────────────────────────────────┬───────────────┘
                │ writes                                  │ produces
                ▼                                          ▼
      ┌──────────────────┐                       ┌──────────────────────┐
      │  Postgres         │                       │  Kafka                │
      │  jobs,             │                       │  transcoding-jobs     │
      │  job_status_events │                       │  transcoding-dlq      │
      └─────────▲──────────┘                       └───────────┬───────────┘
                │ reads/writes status                            │ consume (group: pulsegrid-workers)
                │                                                 ▼
                │                                    ┌────────────────────────┐
                │                                    │  Worker pod(s) (:8081)  │
                │                                    │  1. Download source S3  │
                └────────────────────────────────────┤  2. ffmpeg → MP4/HLS    │
                                                       │  3. Generate manifest   │
                                                       │  4. Upload outputs S3   │
                                                       │  5. Commit offset /     │
                                                       │     retry / DLQ / exit  │
                                                       └───────────┬─────────────┘
                                                                    │
                                                                    ▼
                                                       ┌────────────────────────┐
                                                       │  S3 (source + output)   │
                                                       └────────────────────────┘

Both API and workers expose /metrics → scraped by Prometheus → Grafana dashboard + alert rules.
```

Job outcome routing in the worker:

```
process job
   │
   ├─ success ───────────────────────────────► commit offset, emit completed metric
   │
   ├─ resource constraint (OOM/disk full) ────► emit metric, os.Exit(1) → k8s restarts pod,
   │                                             Kafka rebalances, message redelivered
   │
   ├─ permanent (corrupt source, bad codec) ──► publish to DLQ, commit offset
   │
   └─ retryable (network, S3 5xx, timeout)
          ├─ retry_count < 3 ──► increment count, re-publish to same topic, commit offset
          └─ retry_count ≥ 3 ──► publish to DLQ, commit offset
```

## Tech Stack

- **Language**: Go 1.25
- **HTTP**: stdlib `net/http` only — no framework, to keep the binary small and dependency surface minimal
- **Queue**: Kafka via `segmentio/kafka-go` (deployed in-cluster, not MSK)
- **Database**: PostgreSQL via `jackc/pgx/v5` (pgxpool), no ORM
- **Object storage**: S3 via `aws-sdk-go-v2` (`s3/manager` multipart uploader)
- **Transcoding**: `ffmpeg` invoked as a subprocess
- **Metrics**: `prometheus/client_golang`, visualized in Grafana; alerting via Prometheus rules
- **Logging**: structured JSON logging via `go.uber.org/zap`
- **Infra**: Docker, Kubernetes (EKS), Terraform (VPC/EKS/RDS/S3), KEDA for worker autoscaling

## Installation

```bash
git clone <repo-url>
cd pulsegrid
go build ./...
```

Prerequisites:

- Go 1.25+
- `ffmpeg` on `PATH` (worker shells out to it; verify with `ffmpeg -version`)
- PostgreSQL, Kafka, and S3-compatible storage for a full end-to-end run (all three are optional for local dev — see below)

## Configuration

Both binaries read configuration from environment variables. Every external dependency is optional — if unset, that integration is skipped and the binary runs in a degraded "local mode" (useful for iterating without standing up infra).

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `DATABASE_URL` | api | — | Postgres connection string; enables DB writes + runs migrations on startup |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | api, worker | — | Enables S3 upload/download |
| `PULSEGRID_SOURCE_BUCKET` | api, worker | `pulsegrid-source` | S3 bucket for source videos |
| `KAFKA_BROKERS` | api, worker | `localhost:9092` (worker) | Comma-separated broker list; enables Kafka |
| `KAFKA_TOPIC` / `JOB_TOPIC` | api / worker | `transcoding-jobs` | Main job topic |
| `KAFKA_DLQ_TOPIC` / `DLQ_TOPIC` | api / worker | `transcoding-dlq` | Dead-letter topic |
| `KAFKA_CONSUMER_GROUP` / `CONSUMER_GROUP` | api / worker | `pulsegrid-workers` | Consumer group for queue-depth polling and job consumption |

A working set of these lives in `.demo-env` for local scripting (not committed with real credentials).

## Running locally

Without any env vars set, both binaries start in local mode: the API accepts uploads (returning synthetic S3 URIs) and the worker no-ops on the S3/Kafka steps — useful for exercising the HTTP surface and handler logic without infra.

```bash
# API server on :8080
go run ./cmd/api

# Worker (metrics on :8081) — separate terminal
go run ./cmd/worker
```

For a real end-to-end run, export `DATABASE_URL`, `KAFKA_BROKERS`, and `AWS_REGION` (see `.demo-env`) before starting each process. Postgres migrations in `db/migrations/` run automatically on API startup when `DATABASE_URL` is set.

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/pulsegrid go run ./cmd/api
```

Or build and run the container images directly:

```bash
make docker-build
docker run -p 8080:8080 --env-file .demo-env ghcr.io/pulsegrid/api:dev
docker run -p 8081:8081 --env-file .demo-env ghcr.io/pulsegrid/worker:dev
```

## Example usage

Submit a video for transcoding:

```bash
curl -X POST http://localhost:8080/videos/upload \
  -F "source_name=my-video" \
  -F "video=@sample.mp4" \
  -F 'renditions=[{"id":"720p","resolution":"1280x720","video_codec":"libx264","video_bitrate":"5M","audio_codec":"aac","audio_bitrate":"128k"}]'
```

```json
{
  "job_id": "b3f1c2...-uuid",
  "status_uri": "/jobs/b3f1c2...-uuid",
  "estimated_wait_time_seconds": 120,
  "submission_time": "2026-07-15T10:30:00Z"
}
```

Poll for status:

```bash
curl http://localhost:8080/jobs/b3f1c2...-uuid
```

List recent completed jobs:

```bash
curl "http://localhost:8080/jobs?status=completed&limit=20"
```

Health check (used for k8s liveness/readiness):

```bash
curl http://localhost:8080/health
```

Full request/response schema, error codes, and metric definitions are in [docs/api.md](docs/api.md).

## Project Structure

```
pulsegrid/
├── cmd/
│   ├── api/            # API server entrypoint: HTTP handlers, upload flow, job queries
│   ├── worker/         # Worker entrypoint: Kafka consume loop, transcode orchestration, retry/DLQ routing
│   └── load-test/      # Standalone load-test harness (submits jobs, polls status, emits SLO report)
├── pkg/                # Shared library code
│   ├── s3client.go      # S3 upload/download with retry + tagging
│   ├── kafka.go         # Kafka producer/DLQ client
│   ├── queue/           # Higher-level MessageQueue abstraction (produce+consume+commit+DLQ)
│   ├── postgres.go      # pgxpool client, job CRUD, transactional insert
│   ├── transcode.go     # ffmpeg invocation for MP4 and HLS renditions
│   ├── manifest.go       # Output manifest generation
│   ├── errors.go        # Error classification (retryable/permanent/constraint)
│   ├── retry.go         # Generic exponential-backoff utility
│   ├── metrics.go        # Prometheus metric definitions
│   ├── logger.go         # Structured (zap) JSON logging
│   └── models.go         # Core domain types: Job, Rendition, JobStatus, OutputFile
├── db/
│   ├── migrate.go        # Migration runner, invoked on API startup
│   └── migrations/       # Versioned SQL schema (jobs, job_status_events, status enum changes)
├── test/integration/     # End-to-end tests, including a real (non-mocked) ffmpeg transcode test
├── kube/                 # Kubernetes manifests: API/worker deployments, RBAC, ConfigMap, Grafana dashboard, Prometheus alert rules
├── terraform/            # AWS infrastructure as code: VPC, EKS, RDS, S3 buckets, remote state bootstrap
├── docs/                 # API reference and design notes
├── Dockerfile.api / Dockerfile.worker   # Container images for each service
└── Makefile              # build / test / lint / docker-build / docker-push / ci targets
```

## Design Decisions

- **Stdlib `net/http` over a framework.** No gin/chi/echo — smaller binary, no third-party routing surface to reason about for a service with four endpoints.
- **Atomic DB→Kafka→DB write ordering.** A naive "write DB, then publish" or "publish, then write DB" can leave an orphan (job queued but unknown to the API, or vice versa). Inserting with `status=submitting`, publishing to Kafka, then updating to `submitted` and committing means a Kafka failure rolls back cleanly, and a post-publish commit failure is at least detectable (logged as an operator alert) rather than silently inconsistent.
- **Everything behind interfaces (`S3Uploader`, `KafkaProducer`, `DBClient`).** Each is nil-safe, so the binaries degrade gracefully to a "local mode" when a dependency isn't configured — this made local development and unit testing possible without standing up Postgres/Kafka/S3, at the cost of an implicit runtime mode that has to stay documented (see Configuration).
- **Three-tier error classification instead of one retry policy.** Transient failures (network, S3 throttling) are worth retrying; permanent failures (corrupt source, unsupported codec) are not and should route straight to the DLQ; resource exhaustion (OOM, disk full) is a symptom of the pod itself being unhealthy, so the correct action is to exit and let Kubernetes reschedule, not to retry the same pod against the same disk.
- **Re-enqueue to the same topic rather than a dedicated retry topic.** Simpler operationally — `retry_count` lives in the message body — at the cost of retried messages competing for the same partition ordering as fresh work. Acceptable given jobs are already keyed and processed independently.
- **Kafka in-cluster instead of MSK.** Keeps AWS footprint (and cost) down; the tradeoff is the team owns broker operations instead of a managed service.
- **No local disk buffering on upload.** The API streams the multipart body straight into an S3 multipart upload, so 10GB uploads don't require 10GB of API pod disk/memory.

## Challenges

- **Avoiding orphaned jobs across two systems with no shared transaction.** Postgres and Kafka can't be committed atomically; the three-step write ordering (see above) was the mechanism chosen to make failures detectable and mostly self-healing rather than silent.
- **Classifying ffmpeg failures correctly.** ffmpeg's stderr doesn't cleanly separate "this codec isn't supported" from "the disk filled up." `pkg.ClassifyError` combines typed Go errors with string-pattern fallbacks, and a recent fix pass ("Fix job submission and HLS transcode bugs found during live infra test") addressed cases the initial classification missed under real load.
- **Testing infrastructure-dependent code without infrastructure.** Interfaces for S3/Kafka/DB let most of the codebase be unit tested with mocks, but that leaves a gap between "unit tests pass" and "actually works against real Kafka/Postgres/ffmpeg." `test/integration/ffmpeg_e2e_test.go` closes part of that gap by running a real (non-mocked) transcode; the implementation audit tracks remaining gaps in property/unit test coverage for consumer lifecycle, S3 download, and DLQ paths.
- **Backpressure and autoscaling signals.** Queue depth isn't directly exposed by Kafka in a form Kubernetes/KEDA can consume cheaply, so the API polls consumer-group lag every 30s in a background goroutine and exposes it as a gauge for both dashboards and scaling decisions.

## Future Improvements

- Close out the unit/property test gaps identified in `implementation-audit.md` (consumer lifecycle, S3 download error paths, DLQ entry properties, queue schema).
- Move Kafka to a managed service (MSK) or evaluate replacing it if in-cluster operational cost outweighs the AWS spend, per `aws-architecture-review.md`.
- Add authentication/authorization to the API — currently any caller with network access can submit and query jobs.
- Add a manifest/output validation step post-transcode (checksum or playback smoke-test) before marking a job complete.
- Support resumable/chunked uploads for very large sources instead of a single long-lived multipart POST.

## Screenshots

_No screenshots committed to the repository yet. Suggested additions:_

- Grafana dashboard (`kube/grafana-dashboard.json`) showing queue depth, p50/p95/p99 latency, and DLQ backlog panels.
- `curl` walkthrough of submit → poll → completed job with output URLs.
- Kubernetes `kubectl get pods` showing worker autoscaling under load-test burst traffic.

## Resume Bullet Points

- Designed and implemented an atomic cross-system write protocol (Postgres transaction + Kafka publish) that eliminates orphaned job records under partial-failure conditions, without requiring distributed transactions.
- Built a three-tier error classification and recovery system (retryable / permanent / resource-constraint) for a Kafka-consuming worker fleet, driving distinct re-enqueue, dead-letter, and pod-exit behavior to prevent poison-pill jobs from stalling the queue.
- Instrumented a distributed transcoding pipeline end-to-end with Prometheus metrics, Grafana dashboards, and alert rules (queue depth, DLQ backlog, p99 latency), and validated system behavior under load with a custom load-test harness producing SLO reports.
