# PulseGrid Demo Script

Verified working end-to-end against real AWS infra on 2026-07-15 (RDS Postgres, S3, EKS-adjacent). Two terminals needed (API + worker); optional third for curl/watch.

## 0. Pre-check (once, before demo)

```bash
go version          # need 1.25+
ffmpeg -version      # worker shells out to it
```

## AWS infra status (dev, us-east-1, account 534120920769)

Deployed via `terraform/` — full stack live:

| Resource | Status | Detail |
|---|---|---|
| VPC + 2 public/2 private subnets, NAT gateway | up | `vpc-0b5dc9339441c07d0` |
| EKS cluster `pulsegrid-dev` | ACTIVE | k8s 1.31 |
| EKS node groups `pulsegrid-dev-api` / `-worker` | ACTIVE | 1x `t3.small` each (account is Free-Tier restricted, `t3.large` failed to launch) |
| RDS Postgres `pulsegrid-dev-postgres` | available | `db.t4g.micro`, private subnet, SSL required |
| S3 buckets | up | `pulsegrid-dev-source` (source), `pulsegrid-output` (output — literal name, hardcoded in `pkg/s3client.go`, not `pulsegrid-dev-output`) |

Kafka is **not** deployed to the cluster (no manifest exists for it in `kube/`, KEDA not installed, no ECR images pushed). Demo runs API/worker as **local Go processes** talking to the real RDS/S3 and a **local Kafka broker** — this is the tested, reliable path. Full in-cluster deploy (Kafka StatefulSet + KEDA + ECR + `kubectl apply -f kube/`) is a separate, bigger effort not needed for this demo.

### One-time infra connection setup

RDS sits in a private subnet with no public IP — unreachable directly from your laptop. Tunnel through a pod in the cluster instead (safer than opening RDS to the internet):

```bash
aws eks update-kubeconfig --name pulsegrid-dev --region us-east-1
kubectl run pg-tunnel --image=alpine/socat --restart=Never -- \
  tcp-listen:5432,fork,reuseaddr tcp-connect:pulsegrid-dev-postgres.cwximsemyaod.us-east-1.rds.amazonaws.com:5432
kubectl port-forward pod/pg-tunnel 15432:5432 &
```

Use port **15432**, not 5432 — port 5432 is very likely already taken by a local Postgres install (`kubectl port-forward` fails silently-ish on Windows if the port's busy; check `netstat -ano | grep 5432` first). If `pg-tunnel` pod already exists from a prior session, skip the `kubectl run`.

DB password lives in `terraform/terraform.tfvars` (gitignored, not in repo, generated during infra setup). If the live RDS instance predates the current tfvars password (e.g. imported from an old apply), reset it: `aws rds modify-db-instance --db-instance-identifier pulsegrid-dev-postgres --master-user-password '<pass-from-tfvars>' --apply-immediately`.

## 1. Build

```bash
go build ./...
```

## 2. Env vars (real infra)

```bash
export DATABASE_URL="postgres://pulsegrid:<PASSWORD_FROM_TFVARS>@localhost:15432/pulsegrid?sslmode=require"
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=transcoding-jobs
export KAFKA_DLQ_TOPIC=transcoding-dlq
export KAFKA_CONSUMER_GROUP=pulsegrid-workers
export AWS_REGION=us-east-1
export PULSEGRID_SOURCE_BUCKET=pulsegrid-dev-source
```

Kafka must be reachable at `localhost:9092` (KRaft single-node is fine) before starting the API — it fails fast on a bad broker connection. Migrations run automatically once the API starts.

**Before starting anything**, kill stray processes from earlier runs — `go run` on Windows leaves orphaned `api.exe`/`worker.exe` children that keep old ports/registries alive after you Ctrl+C the parent, causing confusing symptoms (metrics stuck at 0, wrong process answering `/health`):

```bash
tasklist | grep -i -E "api\.exe|worker\.exe"
# taskkill //F //PID <pid> for anything found
```

## 3. Terminal A — start API server

```bash
go run ./cmd/api
```

Listens :8080. Wait for `pulsegrid api server starting on :8080` before next step.

## 4. Terminal B — start worker

```bash
go run ./cmd/worker
```

Listens :8081 (metrics), consumes `transcoding-jobs`.

## 5. Terminal C — sanity check health

```bash
curl http://localhost:8080/health
```

Expect all three `"healthy"`: `{"status":"healthy","checks":{"kafka":{"status":"healthy"},"postgres":{"status":"healthy"},"s3":{"status":"healthy"}}}`

## 6. Submit a job — use the short clip, not `sample.mp4`

`sample.mp4` is 43MB/46s and takes **6+ minutes** end-to-end (mostly S3 upload time on typical home bandwidth) — too slow for a live recording. A trimmed 8s clip (`demo-clip-short.mp4`, already in repo root, ~7.6MB) finishes in **~90 seconds**. If it's missing, regenerate: `ffmpeg -y -i sample.mp4 -t 8 -c copy demo-clip-short.mp4`.

```bash
curl -X POST http://localhost:8080/videos/upload \
  -F "video=@demo-clip-short.mp4" \
  -F "source_name=demo-video"
```

Expect HTTP 202, `job_id` in response — copy it, needed next step.

## 7. Query job status

```bash
curl http://localhost:8080/jobs/<job_id>
```

**Known gap, say this out loud during demo:** worker doesn't write completion status back to Postgres yet — status stays at submission-time value (`submitted`), won't flip to `completed` even though the job fully succeeds. Verified still true. Use worker logs/metrics (step 8) as the real completion signal, not this endpoint.

## 8. Show worker doing real work

Watch Terminal B logs — you'll see, in order: `job_processing_start` → S3 download → `rendition_completed` (720p) → `rendition_completed` (480p) → `hls_rendition_completed` → S3 uploads → `cleanup_complete` → `job_completed`. Then:

```bash
curl http://localhost:8081/metrics | grep pulsegrid_job_completed_total
```

Counter increments on success (verified: 3 renditions — 720p MP4, 480p MP4, HLS segments — all upload to the real `pulsegrid-output` S3 bucket).

## 9. List jobs (filtered)

```bash
curl "http://localhost:8080/jobs?status=submitted&limit=10"
```

## 10. Show metrics / observability story

```bash
curl http://localhost:8080/metrics | grep pulsegrid_
curl http://localhost:8081/metrics | grep pulsegrid_
```

Mention Grafana dashboard (`kube/grafana-dashboard.json`) and alert rules (`kube/prometheus-rules.yaml`) — describe panels (queue depth, p50/p95/p99 latency, failure rate, DLQ backlog) since they're not deployed to a live Grafana instance.

## 11. (Optional) Load test burst

```bash
go run ./cmd/load-test \
  -base-url http://localhost:8080 \
  -num-jobs 20 \
  -video-size 5MB \
  -burst-duration 10s \
  -target-renditions 2 \
  -output-dir demo-load-results
```

Then show `demo-load-results/summary.md`.

## 12. (Optional) Real ffmpeg e2e test as proof

```bash
go test ./test/integration -run TestE2E_RealFFmpegTranscode -v
```

## 13. Wind down

Ctrl+C Terminal A (API), Ctrl+C Terminal B (worker) — then verify no orphaned `api.exe`/`worker.exe` survive (see step 2 note) before your next run. Leave `pg-tunnel` pod and port-forward running if you'll demo again soon; otherwise `kubectl delete pod pg-tunnel` and kill the port-forward.

---

## Fixes applied to get this working (for context, not to redo)

- **DB migration bug**: `jobs.status` CHECK constraint was missing `'submitting'`, the transient state the API writes before Kafka confirms — every job submission failed until `db/migrations/003_add_submitting_status.sql` was added. This was never caught before because local demo mode always skipped real Postgres.
- **HLS transcode bug**: `cmd/api/main.go` set the HLS rendition's `BaseResolution: "720p"`, which ffmpeg rejects as an invalid `-s` value (must be `1280x720` or a named preset like `hd720`) — every job failed on the 3rd rendition and retried into the ground. Fixed to `"1280x720"`.
- Both fixes are in the working tree, not yet committed.

## Fallback if no real infra available

Skip env vars entirely — API/worker fall back to local no-op mode, uploads return 202 without persistence/queueing. Run only steps 1, 3, 5, 6, 10. Frame it as "HTTP layer + validation + S3 upload path" demo, not full pipeline.
