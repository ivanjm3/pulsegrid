# Pulsegrid

Pulsegrid is a distributed video transcoding platform built in Go. Clients upload source videos through an HTTP API, the API stores the source in S3, records job state in Postgres, and enqueues work to Kafka. Worker pods consume jobs, download the source, transcode renditions with ffmpeg, upload outputs and manifests to S3, and emit Prometheus metrics for operational visibility.

The system is designed to be testable end to end. The repository includes unit tests, integration tests, a load-test harness, Kubernetes manifests, Terraform infrastructure, Grafana dashboards, and Prometheus alert rules.

## High-Level Flow

1. Client submits `POST /videos/upload` with a multipart form body.
2. API validates the request, uploads the source to S3, writes job metadata to Postgres, and publishes the job to Kafka.
3. Worker pods consume the Kafka message, download the source, transcode renditions, generate a manifest, upload all outputs to S3, and update job status.
4. API serves `GET /jobs/{job_id}` for status and output lookup.
5. Prometheus scrapes API and worker metrics; Grafana visualizes queue depth, latency, failures, and DLQ growth; alert rules notify on sustained issues.

## Repository Layout

```
pulsegrid/
├── cmd/
│   ├── api/            # API server entrypoint and handler tests
│   ├── load-test/      # Load-test harness and validation tests
│   └── worker/        # Worker pod entrypoint and functional tests
├── db/
│   ├── migrate.go      # Migration runner
│   └── migrations/     # SQL schema migrations
├── docs/               # API documentation and supporting notes
├── kube/               # Kubernetes manifests, Grafana dashboard, alerts
├── pkg/                # Shared types, metrics, retry, S3, Kafka, Postgres
├── terraform/          # AWS infrastructure as code
├── test/integration/   # End-to-end integration tests
└── go.mod
```

## Core Domain Types

- `Job` represents a transcoding job across the full lifecycle.
- `Rendition` describes a desired output profile, including normal MP4 renditions and HLS variants.
- `JobStatus` covers the lifecycle states: submitting, submitted, processing, completed, and failed.
- `RetryConfig` centralizes retry policy for transient failures.
- `OutputFile` captures per-output metadata for job status responses and manifests.

## Error Model

- `TranscodingError` represents ffmpeg or encode failures that may be retryable depending on context.
- `ResourceConstraintError` represents pod-fatal conditions such as disk exhaustion or OOM pressure.
- API handlers return structured error JSON with an `error_code`, `request_id`, and timestamp for easier tracing.

## Metrics and Observability

The project exports Prometheus metrics from both the API server and worker pods.

API metrics:

- `pulsegrid_jobs_submitted_total`
- `pulsegrid_upload_duration_seconds`
- `pulsegrid_queue_depth_jobs`

Worker metrics:

- `pulsegrid_job_completed_total`
- `pulsegrid_transcode_failures_total`
- `pulsegrid_transcode_duration_seconds`
- `pulsegrid_pod_resource_constrained`

Grafana dashboard panels and Prometheus alert rules live in `kube/`:

- [kube/grafana-dashboard.json](kube/grafana-dashboard.json)
- [kube/prometheus-rules.yaml](kube/prometheus-rules.yaml)

The dashboard tracks queue depth, pod counts, p50/p95/p99 latency, failure rate, DLQ backlog, completed throughput, and per-rendition performance. Alerts cover high queue depth, high failure rate, high p99 latency, DLQ backlog, and pod resource constraints.

## API Server

The API server listens on port 8080. It accepts uploads at `POST /videos/upload`, serves status at `GET /jobs/{job_id}`, exposes `GET /jobs` for filtered queries, and returns health data at `GET /health`.

Key behaviors:

- Enforces a 10GB upload limit.
- Validates `source_name`, the video file, and rendition JSON.
- Uploads the source to S3 without local buffering.
- Uses the DB/Kafka ordering that prevents orphaned jobs.
- Emits submission and duration metrics on successful uploads.

See [docs/api.md](docs/api.md) for request and response details.

## Worker Pod

The worker process listens on port 8081 for metrics and consumes Kafka jobs from the `transcoding-jobs` topic.

Worker responsibilities:

- Download the source from S3 to local staging.
- Run ffmpeg transcodes for single renditions and HLS variants.
- Generate a manifest describing all outputs.
- Upload outputs and the manifest back to S3.
- Commit Kafka offsets only after successful processing.
- Emit structured logs and worker metrics.
- Clean up temporary files after the job finishes.

## Load Test Harness

The `cmd/load-test` package is a standalone harness for exercising the API and status flow.

Capabilities:

- Configurable job count, source size, burst duration, and target rendition count.
- Multipart job submission against `POST /videos/upload`.
- Polling of `GET /jobs/{job_id}` until terminal state.
- Metrics scraping for queue and scaling signals.
- JSON report generation plus a markdown SLO summary.

Example:

```bash
go run ./cmd/load-test \
    -base-url http://localhost:8080 \
    -num-jobs 100 \
    -video-size 25MB \
    -burst-duration 30s \
    -target-renditions 3 \
    -output-dir load-test-results
```

The harness writes `report.json` and `summary.md` to the output directory.

## Build and Test

```bash
go build ./...
go test ./...
```

Useful targeted tests:

```bash
go test ./cmd/api
go test ./cmd/worker
go test ./cmd/load-test
go test ./test/integration
```

## Local Development

Run the API server:

```bash
go run ./cmd/api
```

Run the worker:

```bash
go run ./cmd/worker
```

When running locally, you can omit external services and the binaries will fall back to a limited local mode if environment variables are not configured.

## Database Migrations

Database migrations live in `db/migrations/` and are run on API startup when `DATABASE_URL` is set.

- `001_create_jobs_table.sql` creates the jobs table and supporting indexes.
- `002_create_job_status_events.sql` creates the job status events table.

Migration execution is tracked in `schema_migrations`. Repeated startup skips already-applied migrations.

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/pulsegrid go run ./cmd/api
```

## Kubernetes Deployment

Manifests live in `kube/` and include the API deployment, worker deployment, ConfigMap, RBAC, namespace, Grafana dashboard, and alert rules.

Important files:

- [kube/api-deployment.yaml](kube/api-deployment.yaml)
- [kube/worker-deployment.yaml](kube/worker-deployment.yaml)
- [kube/configmap.yaml](kube/configmap.yaml)
- [kube/rbac.yaml](kube/rbac.yaml)
- [kube/prometheus-rules.yaml](kube/prometheus-rules.yaml)

The worker deployment includes the Prometheus scrape annotations and KEDA scaling configuration. The API deployment exposes both application traffic and metrics scraping.

## Terraform Infrastructure

Terraform code in `terraform/` provisions the AWS environment:

- VPC and subnets
- EKS cluster and node groups
- S3 source and output buckets
- RDS Postgres
- Remote state bootstrap resources

Typical workflow:

```bash
cd terraform/state-bootstrap
terraform init
terraform apply

cd ..
terraform init
terraform plan -var-file=environments/dev.tfvars -var="db_password=<secret>"
terraform apply -var-file=environments/dev.tfvars -var="db_password=<secret>"
```

## Integration and E2E Validation

The repository includes a full lifecycle integration test in [test/integration/full_lifecycle_test.go](test/integration/full_lifecycle_test.go) and a 100-job load-style validation in [cmd/load-test/harness_test.go](cmd/load-test/harness_test.go).

These checks verify the system from upload through worker processing, status query, metrics emission, and output upload.

## Requirements and Design Docs

- [docs/api.md](docs/api.md) documents the HTTP API and metrics.

## Status

This repository currently includes:

- API server and worker implementations
- Kafka, S3, Postgres, and retry utilities
- Prometheus metrics and Grafana/alerting assets
- Kubernetes manifests and Terraform infrastructure
- Integration tests and a load-test harness

## Notes

- Kafka is deployed in-cluster rather than through MSK.
- The project uses structured logs and explicit retry classification.
- Temporary worker files are cleaned up after every job path.
