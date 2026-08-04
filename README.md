# Pulsegrid

Distributed video transcoding platform. Clients upload a source video and a
list of desired output renditions; the platform transcodes them asynchronously
via a Kafka-backed job queue and worker pool, and tracks job lifecycle events
for analytics.

## Architecture

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

- **API server** (`cmd/api`) — accepts uploads, writes the source to S3,
  enqueues a job to Kafka, persists job metadata to Postgres, and serves
  status/range/analytics queries.
- **Worker** (`cmd/worker`) — consumes `transcoding-jobs`, downloads the
  source from S3, runs ffmpeg per rendition (including HLS), uploads outputs
  + manifest to S3, classifies errors (retryable / permanent / resource
  constraint) for retry or DLQ, and publishes job lifecycle events.
- **Analytics consumer** (`cmd/analytics-consumer`) — consumes
  `job-lifecycle-events`, sinks them into an isolated `analytics` Postgres
  schema, and periodically refreshes materialized views used for
  throughput/latency/failure/rendition reporting.
- **Load test harness** (`cmd/load-test`) — drives the API with a batch of
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
db/migrations/          SQL migrations (jobs, status events, analytics schema/views)
kube/                   Kubernetes manifests (Deployments, KEDA ScaledObject, RBAC)
terraform/              EKS/RDS/S3/VPC infrastructure
monitoring/             Grafana dashboard + Prometheus alert rules
tests/                  Checkpoint and integration tests
```

## Building and testing

```
make build   # go build ./...
make test    # go test ./...
```

Docker images:

```
make docker-build   # builds api, worker, analytics-consumer images
make docker-push
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

Run migrations in `db/migrations/` in order (001–004) before starting any
service against a fresh database.

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
