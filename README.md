# Pulsegrid

Distributed video transcoding platform. Upload a source video and a list of
desired output renditions; Pulsegrid transcodes them asynchronously over a
Kafka-backed job queue and worker pool, then tracks job lifecycle events for
analytics.

## Quick start

```
make build              # go build ./...
make test               # go test ./...
make docker-build       # build api, worker, analytics-consumer images
```

Requires Kafka, Postgres, and S3-compatible storage. Run migrations in
`db/migrations/` (001-004, in order) against a fresh database before starting
any service. See [Running locally](#running-locally) for required env vars.

## How it works

```
                 ┌─────────────┐        ┌──────────────────┐
  client ───────▶│  API server │───────▶│  Kafka            │
                 │  (cmd/api)  │        │  transcoding-jobs │
                 └──────┬──────┘        └─────────┬────────┘
                        │                          │
                        ▼                          ▼
                 ┌─────────────┐          ┌────────────────┐
                 │  Postgres   │◀─────────│  Worker pod(s)  │
                 │  (jobs,     │  status  │  (cmd/worker)   │
                 │  status_    │  events  │  ffmpeg         │
                 │  events)    │          └────────┬────────┘
                 └─────────────┘                   │
                        ▲                           │ S3 (source/output)
                        │                           ▼
                 ┌──────┴────────┐          ┌───────────────┐
                 │ Analytics     │◀─────────│ job-lifecycle- │
                 │ consumer      │  Kafka   │ events topic   │
                 │ (Postgres     │          └───────────────┘
                 │ analytics     │
                 │ schema +      │
                 │ matviews)     │
                 └───────────────┘
```

1. A client uploads a source video and desired renditions to the **API
   server**, which stores the source in S3, writes job metadata to Postgres,
   and enqueues a job on the `transcoding-jobs` Kafka topic.
2. A **worker** picks up the job, downloads the source, runs ffmpeg per
   rendition (including HLS), and uploads outputs + manifest to S3. Errors
   are classified as retryable, permanent, or resource-constraint, driving
   retry or dead-letter-queue handling. Each state transition is published
   to the `job-lifecycle-events` topic.
3. The **analytics consumer** reads that topic, sinks events into an
   isolated `analytics` Postgres schema, and periodically refreshes
   materialized views for throughput/latency/failure/rendition reporting —
   served back through the API's `/analytics/summary` endpoint.

A **load test harness** (`cmd/load-test`) drives the API with a batch of
jobs and reports latency percentiles / success rate against SLOs.

Design and requirements detail lives in `.spec/` (`design.md`,
`requirements.md`, `tasks.md`, `CHANGELOG.md`).

## Repository layout

```
cmd/
  api/                  API server entrypoint
  worker/               Worker pod entrypoint
  analytics-consumer/   Analytics consumer entrypoint
  load-test/            Load test harness
pkg/
  api/                  HTTP handlers, request/response types
  worker/               Transcoding pipeline, retry/DLQ, lifecycle events
  analytics/            Analytics sink, views, consumer logic
  queue/                Kafka producer/consumer abstraction (pkg/queue/kafka.go)
  store/                Postgres access (jobs, status events)
  storage/              S3 client
  metrics/              Prometheus metric definitions
  errors.go, retry.go, types.go   Shared error types, retry policy, domain types
db/migrations/          SQL migrations (jobs, status events, analytics schema/views)
kube/                   Kubernetes manifests (Deployments, KEDA ScaledObject, RBAC)
terraform/              EKS/RDS/S3/VPC infrastructure
monitoring/             Grafana dashboard + Prometheus alert rules
tests/                  Checkpoint and integration tests
```

## Running locally

Each service needs Kafka, Postgres, and (for the API/worker) S3-compatible
storage reachable via the env vars below. Defaults assume Kafka on
`localhost:9092`; there is no default for the DB DSNs — they must be set.

**API server** (`cmd/api`, HTTP on `:8080`, metrics on `:8081`)

| Env var | Default | Purpose |
|---|---|---|
| `DB_DSN` | — (required) | Postgres connection string |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `S3_BUCKET_SOURCE` | `pulsegrid-source` | uploaded source videos |
| `S3_BUCKET_OUTPUT` | `pulsegrid-output` | transcoded outputs |

Routes: `POST /videos/upload`, `GET /jobs/{job_id}`, `GET /jobs`,
`GET /health`, `GET /analytics/summary`.

**Worker** (`cmd/worker`, metrics on `:8081`)

| Env var | Default | Purpose |
|---|---|---|
| `DB_DSN` | — (required) | Postgres connection string |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `S3_BUCKET_OUTPUT` | `pulsegrid-output` | transcoded outputs |
| `LIFECYCLE_TOPIC` | `job-lifecycle-events` | lifecycle event topic |
| `HOSTNAME` | `unknown` | pod id used in logs/manifests |

**Analytics consumer** (`cmd/analytics-consumer`, metrics on `:8082`)

| Env var | Default | Purpose |
|---|---|---|
| `ANALYTICS_DB_DSN` | — (required) | Postgres connection string (analytics schema) |
| `ANALYTICS_KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `ANALYTICS_CONSUMER_GROUP` | `pulsegrid-analytics` | Kafka consumer group |
| `LIFECYCLE_TOPIC` | `job-lifecycle-events` | lifecycle event topic |

## Deployment

Kubernetes manifests in `kube/` deploy the API, worker (with a KEDA
ScaledObject for queue-depth-based autoscaling), and analytics consumer.
Terraform in `terraform/` provisions the underlying EKS cluster, node
groups, S3 buckets, RDS Postgres instance, and VPC.

Grafana dashboard and Prometheus alert rules are in `monitoring/`.

## Known gaps

See `.spec/CHANGELOG.md` for the full history. As of the last verification
sweep: end-to-end validation (tasks 33/34/43) was done against a local
Postgres/mocked-Kafka/mocked-S3 substitute, not a live EKS cluster — a real
staging deploy, a 100-job load test, a pod-kill chaos test, and live Grafana
rendering against real Prometheus data are still outstanding.
