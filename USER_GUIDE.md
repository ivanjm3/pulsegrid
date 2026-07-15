# PulseGrid

PulseGrid is a distributed video transcoding platform written in Go. Clients upload source videos through an HTTP API; the API stores the source in S3, records job state in Postgres, and enqueues work to Kafka. Worker pods consume jobs, download the source, transcode renditions with ffmpeg, upload outputs and manifests to S3, and emit Prometheus metrics for operational visibility.

The system is built for horizontal scale on Kubernetes (EKS), with KEDA-based autoscaling driven by Kafka consumer lag, Terraform-managed AWS infrastructure, and a Grafana/Prometheus observability stack.

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running Locally](#running-locally)
- [Running with Docker](#running-with-docker)
- [Running on AWS](#running-on-aws)
- [API Documentation](#api-documentation)
- [Job Processing Flow](#job-processing-flow)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [Deployment](#deployment)
- [Known Limitations](#known-limitations)
- [Future Improvements](#future-improvements)

---

## Features

- **Video upload API** — `POST /videos/upload` accepts multipart video uploads up to 10GB, streamed directly to S3 with no local disk buffering.
- **Configurable renditions** — clients may specify custom output renditions (resolution, codec, bitrate) or rely on sane defaults (720p, 480p, HLS).
- **Atomic job submission** — DB-then-Kafka write ordering with rollback prevents orphaned jobs (a queued job with no database record).
- **Job status querying** — `GET /jobs/{job_id}` for single-job lookup and `GET /jobs` for filtered, paginated listing (time range, status).
- **Distributed worker pool** — Kafka-consumer worker pods that download sources, run ffmpeg transcodes, and upload results, scaling independently of the API tier.
- **Multi-rendition transcoding** — standard MP4 renditions plus HLS (HTTP Live Streaming) segment + playlist generation.
- **Manifest generation** — a `manifest.json` describing all produced outputs per job.
- **Retry and dead-letter queue handling** — three-tier error classification (retryable, permanent, resource-constraint) drives re-enqueue, DLQ routing, or pod exit.
- **Temp file cleanup** — worker-local staging files are removed after every job, success or failure.
- **Prometheus metrics** — submission counts, upload/transcode duration histograms, queue depth gauge, completion/failure counters.
- **Health checks** — `GET /health` aggregates Postgres, Kafka, and S3 connectivity for Kubernetes probes.
- **Grafana dashboard and alert rules** — pre-built panels and Prometheus alerting rules for queue depth, latency, failure rate, and DLQ backlog.
- **KEDA autoscaling** — worker pod count scales 0–30 based on Kafka consumer group lag.
- **Kubernetes manifests** — deployments, ConfigMap, Secrets, RBAC, and namespace definitions for both API and worker tiers.
- **Terraform-managed AWS infrastructure** — VPC, EKS cluster, node groups, S3 buckets, RDS Postgres, and remote state bootstrap.
- **Load-test harness** — standalone tool for burst-submitting jobs and generating JSON/Markdown SLO reports.
- **Local development fallback** — API and worker binaries run without external services when environment variables are unset (S3/Kafka/DB calls are skipped).

---

## Architecture Overview

```
                              ┌─────────────┐
                              │   Client    │
                              └──────┬──────┘
                                     │ POST /videos/upload
                                     ▼
                       ┌───────────────────────────┐
                       │        API Server          │
                       │   (cmd/api, port 8080)     │
                       └───┬───────────┬────────────┘
                           │           │
                 stream    │           │  write job row
                 upload    │           │  (status=submitting)
                           ▼           ▼
                     ┌─────────┐  ┌──────────┐
                     │   S3    │  │ Postgres │
                     │ source  │  │  (RDS)   │
                     │ bucket  │  └────┬─────┘
                     └─────────┘       │ publish job message
                                       ▼
                                 ┌───────────┐
                                 │   Kafka   │
                                 │  topic:   │
                                 │ transcode │
                                 │  -jobs    │
                                 └─────┬─────┘
                                       │ consume
                                       ▼
                       ┌───────────────────────────┐
                       │       Worker Pods          │
                       │ (cmd/worker, port 8081)    │
                       │  scaled by KEDA on lag     │
                       └───┬───────────┬────────────┘
                           │           │
                  download │           │ upload outputs +
                  source   │           │ manifest.json
                           ▼           ▼
                     ┌─────────┐  ┌──────────┐
                     │   S3    │  │   S3     │
                     │ source  │  │  output  │
                     │ bucket  │  │  bucket  │
                     └─────────┘  └──────────┘

              ┌────────────────────────────────────┐
              │     Prometheus  →  Grafana          │
              │  scrapes :8080/metrics (API)        │
              │  scrapes :8081/metrics (Worker)     │
              └────────────────────────────────────┘
```

---

## Tech Stack

**Backend**

| Component | Technology |
|---|---|
| Language | Go 1.25 |
| HTTP server | Go standard library `net/http` (no external web framework) |
| Video transcoding | ffmpeg (invoked as a subprocess) |
| AWS SDK | AWS SDK for Go v2 |
| Kafka client | `github.com/segmentio/kafka-go` |
| Postgres client | `github.com/jackc/pgx/v5/pgxpool` |
| Metrics | `github.com/prometheus/client_golang` |

**Infrastructure**

| Component | Technology |
|---|---|
| Container orchestration | Kubernetes (EKS) |
| Autoscaling | KEDA (Kafka-lag-driven) |
| Containers | Docker (multi-stage Alpine builds) |
| Infrastructure as Code | Terraform |

**Cloud**

| Component | Technology |
|---|---|
| Provider | AWS |
| Compute | EKS (managed node groups for API and worker tiers) |
| Object storage | S3 (source and output buckets) |
| Managed database | RDS (Postgres) |
| Networking | VPC, public/private subnets, NAT gateway |

**Database**

| Component | Technology |
|---|---|
| Primary store | PostgreSQL (via RDS) |
| Time-series events | TimescaleDB hypertable (`job_status_events`) |

**Messaging**

| Component | Technology |
|---|---|
| Queue | Apache Kafka (self-managed in-cluster, not MSK) |
| Topics | `transcoding-jobs` (main), `transcoding-dlq` (dead-letter) |

**Monitoring**

| Component | Technology |
|---|---|
| Metrics collection | Prometheus |
| Dashboards | Grafana |
| Alerting | Prometheus alert rules |

**Testing**

| Component | Technology |
|---|---|
| Unit / functional tests | Go `testing` package |
| Integration tests | `test/integration` package |
| Load testing | Custom harness (`cmd/load-test`) |

---

## Repository Structure

```
pulsegrid/
├── cmd/
│   ├── api/            # API server entrypoint, HTTP handlers, handler tests
│   ├── worker/          # Worker pod entrypoint, Kafka consumer loop, cleanup logic
│   └── load-test/       # Standalone load-test harness and validation tests
├── db/
│   ├── migrate.go       # Migration runner (executed on API startup)
│   └── migrations/      # Versioned SQL schema migrations
├── docs/
│   ├── api.md            # Full HTTP API reference
│   ├── requirements-mapping.md
│   └── interview-notes.md
├── kube/                 # Kubernetes manifests: deployments, ConfigMap, Secrets, RBAC,
│                          # namespace, Grafana dashboard JSON, Prometheus alert rules
├── pkg/                  # Shared library code: models, S3/Kafka/Postgres clients,
│                          # metrics, retry logic, error classification, manifest
│                          # generation, transcoding, queue abstraction (pkg/queue)
├── terraform/             # AWS infrastructure as code (VPC, EKS, RDS, S3, IAM)
│   ├── environments/      # Per-environment tfvars (dev, prod)
│   └── state-bootstrap/   # Remote Terraform state backend resources
├── test/integration/      # End-to-end lifecycle integration test
├── Dockerfile.api          # API server container build
├── Dockerfile.worker       # Worker pod container build (includes ffmpeg)
├── Makefile                # Build, test, lint, and Docker targets
└── go.mod
```

---

## Prerequisites

| Requirement | Version / Notes |
|---|---|
| Go | 1.25 or later |
| ffmpeg | Required by worker at runtime (installed in `Dockerfile.worker`; install locally for local worker runs) |
| Docker | Required for containerized runs and image builds |
| PostgreSQL | With TimescaleDB extension (required for `job_status_events` hypertable) |
| Apache Kafka | Any broker reachable via `KAFKA_BROKERS` |
| AWS account | Required only for S3 upload/download and AWS deployment |
| Terraform | Required only for provisioning AWS infrastructure |
| kubectl | Required only for Kubernetes deployment |
| KEDA | Required only for worker autoscaling on Kubernetes |

All external dependencies are optional for local development — the API and worker fall back to a limited local mode when their corresponding environment variables are unset.

---

## Installation

### Clone

```bash
git clone <repository-url>
cd pulsegrid
```

### Dependencies

```bash
go mod download
```

### Environment Variables

Set only the variables for the services you want active. See [Configuration](#configuration) for the full list and defaults.

```bash
export AWS_REGION=us-east-1
export PULSEGRID_SOURCE_BUCKET=pulsegrid-source
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=transcoding-jobs
export KAFKA_DLQ_TOPIC=transcoding-dlq
export KAFKA_CONSUMER_GROUP=pulsegrid-workers
export DATABASE_URL=postgres://user:pass@localhost:5432/pulsegrid
```

### Configuration

No separate config file is used — all configuration is via environment variables (see [Configuration](#configuration)). Database schema is applied automatically via migrations on API startup once `DATABASE_URL` is set.

---

## Running Locally

1. **Build:**

   ```bash
   go build ./...
   ```

2. **Start the API server:**

   ```bash
   go run ./cmd/api
   ```

   Listens on port `8080` (HTTP API + `/metrics`).

3. **Start a worker (separate terminal):**

   ```bash
   go run ./cmd/worker
   ```

   Listens on port `8081` (`/metrics`), consumes from `transcoding-jobs`.

4. **Verify the API is up:**

   ```bash
   curl http://localhost:8080/health
   ```

   Expected output (no external services configured):

   ```json
   {"status":"healthy","checks":{}}
   ```

   With Postgres/Kafka/S3 configured and reachable, `checks` will list each as `"healthy"`.

5. **Submit a test job:**

   ```bash
   curl -X POST http://localhost:8080/videos/upload \
     -F "video=@sample.mp4" \
     -F "source_name=sample-video"
   ```

   Expected output (HTTP 202):

   ```json
   {
     "job_id": "a1b2c3d4-...",
     "status_uri": "/jobs/a1b2c3d4-...",
     "estimated_wait_time_seconds": 120,
     "submission_time": "2026-07-14T10:30:00Z"
   }
   ```

6. **Check job status:**

   ```bash
   curl http://localhost:8080/jobs/a1b2c3d4-...
   ```

If `KAFKA_BROKERS` and `DATABASE_URL` are unset, uploads still return HTTP 202 but the job is not persisted or queued — useful for exercising the HTTP layer in isolation.

---

## Running with Docker

Build images:

```bash
make docker-build
```

This runs, equivalently:

```bash
docker build --platform linux/amd64 -f Dockerfile.api -t ghcr.io/pulsegrid/api:latest .
docker build --platform linux/amd64 -f Dockerfile.worker -t ghcr.io/pulsegrid/worker:latest .
```

Run the API container:

```bash
docker run -p 8080:8080 -p 8081:8081 \
  -e AWS_REGION=us-east-1 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  -e DATABASE_URL=postgres://user:pass@host.docker.internal:5432/pulsegrid \
  ghcr.io/pulsegrid/api:latest
```

Run the worker container:

```bash
docker run -p 8081:8081 \
  -e AWS_REGION=us-east-1 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  ghcr.io/pulsegrid/worker:latest
```

Expected behavior: both containers run as a non-root `pulsegrid` user. The API image is ffmpeg-free (small runtime, Alpine 3.19). The worker image includes ffmpeg for transcoding. The API exposes 8080 (HTTP) and 8081 (metrics); the worker exposes 8081 (metrics) only.

Push images (requires registry credentials):

```bash
make docker-push
```

---

## Running on AWS

Infrastructure is provisioned with Terraform and deployed to Kubernetes (EKS) with the manifests in `kube/`.

### Required AWS Resources (provisioned by Terraform)

- VPC with public/private subnets across 2–3 AZs, NAT gateway
- EKS cluster with two managed node groups: `api` (default `t3.small`) and `worker` (default `t3.large`, supports SPOT capacity)
- Two S3 buckets: source and output, each with versioning, encryption, lifecycle rules, and public access blocked
- RDS Postgres instance (`db.t4g.micro` by default) with a dedicated DB subnet group and security group
- IAM roles for EKS cluster, node groups, and S3 access
- Remote Terraform state backend (bootstrapped separately)

### Deployment Steps

Order matters — each step depends on the previous one.

1. **Authenticate to AWS first.** Terraform and `aws eks update-kubeconfig` both use the AWS CLI's default credential chain — no credentials hardcoded anywhere in this repo. Log in before touching Terraform:

   ```bash
   aws sso login --profile <your-profile>
   # or: aws configure   (long-lived access key/secret)
   ```

   Verify the identity Terraform will use:

   ```bash
   aws sts get-caller-identity
   ```

   If using an SSO/named profile, export it so Terraform and kubectl pick it up:

   ```bash
   export AWS_PROFILE=<your-profile>
   ```

2. **Bootstrap remote state (one-time, run before the main Terraform config — it creates the S3/DynamoDB backend the main config stores its state in):**

   ```bash
   cd terraform/state-bootstrap
   terraform init
   terraform apply
   ```

3. **Provision infrastructure** (VPC, EKS, RDS, S3 — depends on the backend from step 2):

   ```bash
   cd terraform
   terraform init
   terraform plan -var-file=environments/dev.tfvars -var="db_password=<secret>"
   terraform apply -var-file=environments/dev.tfvars -var="db_password=<secret>"
   ```

   Use `environments/prod.tfvars` for production sizing.

4. **Configure kubectl** against the EKS cluster just created (needs same AWS login from step 1):

   ```bash
   aws eks update-kubeconfig --name <cluster-name> --region <region>
   ```

5. **Apply Kubernetes manifests, in order** (namespace/RBAC/config first — deployments reference them):

   ```bash
   kubectl apply -f kube/namespace.yaml
   kubectl apply -f kube/rbac.yaml
   kubectl apply -f kube/configmap.yaml
   kubectl apply -f kube/secrets.yaml
   kubectl apply -f kube/api-deployment.yaml
   kubectl apply -f kube/worker-deployment.yaml
   ```

   `kube/secrets.yaml` must have real values populated before applying (Kafka/DB credentials).

6. **KEDA autoscaling** (worker tier) requires KEDA installed on the cluster beforehand — install before or right after step 5, must exist before the `ScaledObject` in `kube/worker-deployment.yaml` takes effect. Scales worker pods 0–30 based on `transcoding-jobs` consumer lag (threshold 10, polling every 15s).

### Configuration Notes

- Kafka is deployed in-cluster, not via a managed service (MSK).
- The worker pod's `terminationGracePeriodSeconds` is 1800s (30 minutes) to allow in-flight transcodes to complete before pod termination.
- ConfigMap values (`kube/configmap.yaml`) set Kafka broker addresses, topic names, and default AWS region/bucket — override per environment as needed.

---

## API Documentation

Full reference: [docs/api.md](docs/api.md). Summary below.

### POST /videos/upload

Accepts a multipart video upload and enqueues a transcoding job.

**Request:** `multipart/form-data`

| Field | Type | Required | Description |
|---|---|---|---|
| `video` | file | yes | Video file, max 10GB |
| `source_name` | string | yes | Human-readable name |
| `renditions` | JSON string | no | Array of rendition specs; defaults to 720p/480p/HLS if omitted |

**Response (202):**

```json
{
  "job_id": "uuid-v4",
  "status_uri": "/jobs/{job_id}",
  "estimated_wait_time_seconds": 120,
  "submission_time": "2024-01-15T10:30:00Z"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/videos/upload \
  -F "video=@input.mp4" \
  -F "source_name=my-video" \
  -F 'renditions=[{"id":"1080p","resolution":"1920x1080","video_codec":"libx264","video_bitrate":"8M","audio_codec":"aac","audio_bitrate":"128k"}]'
```

**Errors:**

| Status | error_code | Condition |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Missing field, invalid JSON, bad renditions |
| 413 | `PAYLOAD_TOO_LARGE` | File exceeds 10GB |
| 500 | `INTERNAL_ERROR` | Internal failure |

---

### GET /jobs/{job_id}

Returns status for a single job.

**Example:**

```bash
curl http://localhost:8080/jobs/a1b2c3d4-5678-90ab-cdef-1234567890ab
```

**Errors:** 404 if job not found, 500/503 on DB failure or unavailability.

> Note: the response includes an `output_files` field, but it is not currently populated — see [Known Limitations](#known-limitations).

---

### GET /jobs

Query jobs with optional filters.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `submitted_after` | ISO 8601 | — | Inclusive lower bound |
| `submitted_before` | ISO 8601 | — | Inclusive upper bound |
| `status` | comma-separated | — | `submitted`, `processing`, `completed`, `failed` |
| `limit` | integer | 100 | Max 1000 |
| `offset` | integer | 0 | Pagination offset |

**Response (200):**

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

**Example:**

```bash
curl "http://localhost:8080/jobs?status=completed,failed&limit=50"
```

**Errors:**

| Status | error_code | Condition |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Invalid timestamp, bad status, limit/offset out of range |
| 500 | `INTERNAL_ERROR` | Database query failure |
| 503 | `SERVICE_UNAVAILABLE` | Database not configured |

---

### GET /health

Aggregate health of Postgres, Kafka, and S3.

**Example:**

```bash
curl http://localhost:8080/health
```

**Response (200 — healthy):**

```json
{"status":"healthy","checks":{"postgres":{"status":"healthy"},"kafka":{"status":"healthy"},"s3":{"status":"healthy"}}}
```

**Response (503 — unhealthy):**

```json
{"status":"unhealthy","checks":{"postgres":{"status":"unhealthy","error":"connection refused"},"kafka":{"status":"healthy"},"s3":{"status":"healthy"}}}
```

---

### GET /metrics

Prometheus exposition format. No request parameters. See [Monitoring](#monitoring).

```bash
curl http://localhost:8080/metrics
curl http://localhost:8081/metrics
```

### Common Error Format

All non-2xx JSON responses share this shape:

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

## Job Processing Flow

```
Client        API              Postgres          Kafka            Worker           S3
  │            │                  │                │                │              │
  │ POST       │                  │                │                │              │
  │ /videos/   │                  │                │                │              │
  │ upload    ─▶                  │                │                │              │
  │            │  stream video  ──┼────────────────┼────────────────┼─────────────▶│
  │            │                  │                │                │  (source     │
  │            │                  │                │                │   bucket)    │
  │            │  INSERT job      │                │                │              │
  │            │  status=         │                │                │              │
  │            │  submitting    ─▶│                │                │              │
  │            │                  │                │                │              │
  │            │  publish job message              │                │              │
  │            │  (topic: transcoding-jobs)       ─┼───────────────▶│              │
  │            │                  │                │                │              │
  │            │  UPDATE status=submitted,          │                │              │
  │            │  COMMIT tx     ─▶│                │                │              │
  │            │                  │                │                │              │
  │ 202        │                  │                │                │              │
  │◀───────── ─│                  │                │                │              │
  │            │                  │                │  consume       │              │
  │            │                  │                │  message      ─┼─────────────▶│
  │            │                  │                │                │              │
  │            │                  │                │                │ download     │
  │            │                  │                │                │ source     ◀─│
  │            │                  │                │                │              │
  │            │                  │                │                │ ffmpeg       │
  │            │                  │                │                │ transcode    │
  │            │                  │                │                │ (per         │
  │            │                  │                │                │ rendition)   │
  │            │                  │                │                │              │
  │            │                  │                │                │ generate     │
  │            │                  │                │                │ manifest     │
  │            │                  │                │                │              │
  │            │                  │                │                │ upload       │
  │            │                  │                │                │ outputs +    │
  │            │                  │                │                │ manifest   ─▶│
  │            │                  │                │                │ (output      │
  │            │                  │                │                │  bucket)     │
  │            │                  │                │                │              │
  │            │                  │                │  commit offset │              │
  │            │                  │                │◀───────────────│              │
  │            │                  │                │                │              │
  │            │                  │                │                │ cleanup      │
  │            │                  │                │                │ temp files   │
  │            │                  │                │                │              │
  │ GET        │                  │                │                │              │
  │ /jobs/{id}─▶  query status  ─▶│                │                │              │
  │◀───────── ─│                  │                │                │              │
```

On failure, the worker classifies the error (retryable, permanent, or resource-constraint) and either re-enqueues to `transcoding-jobs` with an incremented retry count, publishes to the `transcoding-dlq` topic, or exits the pod for Kubernetes to restart. See [docs/api.md](docs/api.md#worker-error-classification--retryeq) for the full classification table.

---

## Monitoring

### Prometheus

Both API (`:8080/metrics`) and worker (`:8081/metrics`) pods expose Prometheus exposition-format metrics. Kubernetes manifests include `prometheus.io/scrape` annotations for automatic discovery.

**API metrics:**

| Metric | Type | Description |
|---|---|---|
| `pulsegrid_jobs_submitted_total` | counter | Jobs successfully submitted |
| `pulsegrid_upload_duration_seconds` | histogram | Full upload flow duration |
| `pulsegrid_queue_depth_jobs` | gauge | Unconsumed jobs in `transcoding-jobs` (polled every 30s) |

**Worker metrics:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pulsegrid_job_completed_total` | counter | — | Jobs completed successfully |
| `pulsegrid_transcode_failures_total` | counter | `error_type` | Failures by category (retryable/permanent/constraint) |
| `pulsegrid_transcode_duration_seconds` | histogram | `rendition` | Per-rendition transcode duration |
| `pulsegrid_pod_resource_constrained` | counter | — | Pod exits due to OOM/disk exhaustion |

Histogram buckets: 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0 seconds.

### Grafana

Dashboard definition: [kube/grafana-dashboard.json](kube/grafana-dashboard.json). Panels cover queue depth, pod counts, p50/p95/p99 latency, failure rate, DLQ backlog, completed throughput, and per-rendition performance.

Alert rules: [kube/prometheus-rules.yaml](kube/prometheus-rules.yaml), covering high queue depth, high failure rate, high p99 latency, DLQ backlog growth, and pod resource constraints.

### Health Endpoints

`GET /health` on the API checks Postgres, Kafka, and S3 connectivity with a 5-second timeout per check — used for Kubernetes liveness/readiness probes. Worker liveness/readiness probes check the `/metrics` endpoint on port 8081.

---

## Testing

Run all tests:

```bash
go test ./...
```

Run with verbose output (matches `make test`):

```bash
make test
```

Unit tests only (skip integration):

```bash
make test-unit
```

With race detector:

```bash
make test-race
```

Targeted packages:

```bash
go test ./cmd/api
go test ./cmd/worker
go test ./cmd/load-test
go test ./test/integration
go test ./pkg/...
```

Coverage:

```bash
go test ./... -cover
```

Test suites include unit tests, functional tests (`cmd/worker/worker_functional_test.go`), a full-lifecycle integration test ([test/integration/full_lifecycle_test.go](test/integration/full_lifecycle_test.go)), and a load-style validation test in the load-test harness.

---

## Configuration

All configuration is via environment variables. Every variable is optional — unset variables disable the corresponding integration and the service falls back to local/no-op behavior.

| Variable | Default | Purpose |
|---|---|---|
| `AWS_REGION` | — | Enables S3 upload/download when set |
| `AWS_DEFAULT_REGION` | — | Alternative to `AWS_REGION` |
| `PULSEGRID_SOURCE_BUCKET` | `pulsegrid-source` | S3 bucket for source videos |
| `KAFKA_BROKERS` | — | Comma-separated broker list; enables Kafka when set |
| `KAFKA_TOPIC` | `transcoding-jobs` | Main job queue topic |
| `KAFKA_DLQ_TOPIC` | `transcoding-dlq` | Dead-letter queue topic |
| `KAFKA_CONSUMER_GROUP` | `pulsegrid-workers` | Consumer group (also used for queue depth calculation) |
| `DATABASE_URL` | — | Postgres connection string; enables DB writes and runs migrations on API startup |

Output bucket (`pulsegrid-output`) and Kafka acknowledgment/retry parameters are fixed in code, not environment-configurable.

---

## Troubleshooting

| Problem | Diagnosis |
|---|---|
| `POST /videos/upload` returns 202 but job never appears in `GET /jobs` | `DATABASE_URL` likely unset — DB writes are skipped silently in local mode. Confirm via `GET /health` (`postgres` check will be `not_configured` or absent). |
| Job stuck in `submitted`, never transitions to `processing`/`completed` | Worker-side DB status write-back is not implemented in this codebase — see [Known Limitations](#known-limitations). Check worker logs/metrics (`pulsegrid_job_completed_total`) instead of job status for completion signal. |
| `GET /health` returns 503 | One or more of Postgres/Kafka/S3 is unreachable. Response body's `checks` object identifies which component and includes the error. |
| Upload returns 413 | Video file exceeds the 10GB limit. |
| Upload returns 400 `VALIDATION_ERROR` | Missing `source_name`, missing video file, or malformed `renditions` JSON (each entry needs `id` or `type`). |
| Worker exits unexpectedly | Check for `pulsegrid_pod_resource_constrained` metric increments — indicates OOM or disk exhaustion, which triggers a deliberate `os.Exit(1)` so Kubernetes restarts the pod. |
| Jobs pile up in `transcoding-dlq` | Indicates permanent failures (corrupted video, unsupported codec, source 404) or exhausted retries (3 attempts). Inspect DLQ message `failure_reason` field. |
| `terraform apply` fails on state locking or backend | Ensure `terraform/state-bootstrap` has been applied first — it provisions the remote state backend. |
| Migrations don't run | Migrations only run on API startup when `DATABASE_URL` is set; the worker does not run migrations. |

---

## FAQ

**Do I need Kafka, Postgres, and AWS to run this locally?**
No. All three are optional; the API and worker skip the corresponding integration when its environment variable is unset.

**Which service applies database migrations?**
The API server, on startup, when `DATABASE_URL` is set. The worker does not run migrations.

**What ports do the services use?**
API: 8080 (HTTP + metrics). Worker: 8081 (metrics only).

**Does the worker need AWS credentials?**
Yes, for S3 download of sources and upload of outputs, via standard AWS SDK credential resolution (env vars, instance role, etc.), gated by `AWS_REGION` being set.

**Is Kafka run as a managed service?**
No — deployed in-cluster on Kubernetes, not via AWS MSK.

**How does the worker scale?**
Via KEDA, based on Kafka consumer group lag on `transcoding-jobs` (0–30 replicas, lag threshold 10).

**Where can I see the full endpoint reference?**
[docs/api.md](docs/api.md).

**Does GET /jobs/{job_id} return the list of output files?**
The response includes an `output_files` field, but it is not currently populated by any write path — see [Known Limitations](#known-limitations).

---

## Deployment

Order matters — AWS login before Terraform, Terraform before Kubernetes.

1. Log in to AWS (`aws sso login --profile <profile>` or `aws configure`). Terraform and `kubectl` both reuse these credentials via the AWS CLI default credential chain.

2. Build and push Docker images:

   ```bash
   make docker-build
   make docker-push
   ```

3. Provision AWS infrastructure with Terraform (see [Running on AWS](#running-on-aws)) — bootstrap state first, then apply using `terraform/environments/prod.tfvars`.

4. Point `kubectl` at the target EKS cluster (`aws eks update-kubeconfig`, same AWS credentials as step 1).

5. Apply manifests in order: namespace, RBAC, ConfigMap, Secrets, then API and worker deployments (see [kube/](kube/)).

6. Confirm rollout:

   ```bash
   kubectl -n pulsegrid rollout status deployment/pulsegrid-api
   kubectl -n pulsegrid rollout status deployment/pulsegrid-worker
   ```

7. Verify health and metrics endpoints are reachable, and confirm the Grafana dashboard and Prometheus alert rules are loaded.

---

## Known Limitations

These are documented gaps confirmed against the codebase (see `implementation-audit.md` for full detail):

- **Worker never writes job completion status back to Postgres.** The worker has no Postgres client wired in; on success, failure, retry, or DLQ, it commits the Kafka offset and updates Prometheus metrics, but never calls `RecordStatusEvent` or updates the `jobs` table. As a result, `status`, `completion_time`, and `failure_reason` in Postgres remain at their submission-time values indefinitely.
- **`GET /jobs/{job_id}` does not return output files.** There is no `output_files` column on the `jobs` table and no S3/manifest lookup in the handler; the field in the response is always empty.
- **Some property-based tests specified in the task list are implemented as example-based unit tests instead** (e.g., manifest schema, retry count increment, DLQ entry, queue schema, temp file cleanup) — the underlying logic is tested and covered, but not via randomized/`quick.Check`-style generation.
- **No unit tests for the worker's consumer lifecycle** (consumer group join, SIGTERM handling mid-job, offset-commit-after-success ordering, crash-without-commit redelivery).
- **No unit tests for S3 source download** (`pkg/s3client_test.go` covers uploads extensively but not `DownloadSourceFromS3`).
- **No unit tests for `pkg/logger.go`** — structured log field presence and ffmpeg stderr truncation are unverified by automated tests.
- **No test exercising real applied schema.** Schema tests use mocks rather than a real/test Postgres instance with migrations applied.
- **Kafka runs in-cluster, not via a managed service** — this is an explicit design choice, not a gap, but means Kafka operational burden (upgrades, scaling, durability) is self-managed.

---

## Future Improvements

Based only on gaps identified in the implementation audit — not speculative roadmap items:

- Wire a Postgres client into the worker and call status-update/`RecordStatusEvent` on success, retry, DLQ, and permanent-failure paths so job status in Postgres reflects reality.
- Add an `output_files` column (or equivalent S3/manifest lookup) so `GET /jobs/{job_id}` can return the actual output file list on completion.
- Add unit tests for the worker consumer lifecycle (SIGTERM handling, offset-commit ordering, crash redelivery).
- Add unit tests for `S3Client.DownloadSourceFromS3` (success, retry, permanent 404 failure).
- Add unit tests for `pkg/logger.go` verifying structured log fields and stderr truncation.
- Convert the example-based tests flagged in the audit (manifest schema, retry count, DLQ entry, queue schema, temp cleanup) into randomized property tests where originally specified.
- Add a schema-validation test that runs real migrations against a test Postgres instance and verifies table/index structure via an insert-then-query round trip.
