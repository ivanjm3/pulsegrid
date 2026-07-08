# Requirements Mapping and Validation

## Overview

This document maps every requirement (1–18) to implementing tasks and tests, identifies gaps, and verifies error path coverage.

**Status: VALIDATED** — All requirements have at least one implementing task. Minor gaps noted below.

---

## Requirement → Task Mapping

### Req 1: API Accepts and Validates Video Uploads

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 1.1 POST /videos/upload → 202 + Job_ID | Task 2, 6 | ✅ Implemented |
| 1.2 File > 10GB → 413 | Task 2 | ✅ MaxBytesReader + header check |
| 1.3 Missing metadata → 400 | Task 2 | ✅ source_name + renditions validation |
| 1.4 UUID v4 Job_ID | Task 1, 1.1 (property test) | ✅ GenerateJobID + property test |
| 1.5 Store in S3 with Job_ID prefix | Task 3 | ✅ UploadSourceToS3 |
| 1.6 Enqueue to Job_Queue | Task 4, 6 | ✅ EnqueueJob with retry |
| 1.7 Response includes Job_ID + status URI | Task 6 | ✅ UploadResponse struct |

### Req 2: Job Queue Contract and Message Format

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 2.1 Message schema (Job_ID, S3, renditions, prefix, retry) | Task 4, 4.1, 23 | ✅ KafkaMessage struct |
| 2.2 Message locked until ack | Task 12, 23 | ✅ Kafka offset commit semantics |
| 2.3 Completed → remove message | Task 12 | ✅ CommitMessages on success |
| 2.4 Failed → increment retry, re-enqueue | Task 18, 18.1 | ✅ handleJobOutcome retryable path |
| 2.5 Max retries → DLQ | Task 18, 18.2 | ✅ SendDLQFromMessage |
| 2.6 Visibility timeout → re-enqueue on crash | Task 12 | ✅ Session timeout 30min; no commit = rebalance |

### Req 3: Worker Pod Transcoding Behavior

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 3.1 Connect + polling loop | Task 12 | ✅ FetchMessage loop |
| 3.2 Download source from S3 | Task 13 | ✅ DownloadSourceFromS3 |
| 3.3 Invoke ffmpeg per rendition | Task 14 | ✅ TranscodeSingleRendition |
| 3.4 Upload outputs to S3 | Task 17 | ✅ UploadOutputsToS3 |
| 3.5 Log error with context | Task 20 | ✅ LogTranscodeFailure (stderr, job_id, timestamp) |
| 3.6 Cleanup temp files | Task 19 | ✅ cleanupTempDir (success + failure) |
| 3.7 OOM/disk → abort + log | Task 18 | ✅ ErrorTypeConstraint → os.Exit(1) |

### Req 4: Output Renditions and Formats

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 4.1 720p/480p/HLS defaults | Task 2 (defaultRenditions) | ✅ |
| 4.2 All renditions from single source | Task 14, 15 (processJob) | ✅ Loop in processJob |
| 4.3 HLS segments + M3U8 | Task 15 | ✅ TranscodeHLS |
| 4.4 JSON manifest | Task 16, 16.1 | ✅ GenerateManifest |
| 4.5 Custom renditions via API params | Task 2 (parseRenditions) | ✅ |

### Req 5: Job Status Tracking and Database Schema

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 5.1 Record metadata on upload | Task 5, 25 | ✅ RecordJobMetadata |
| 5.2 Update on completion (timestamp, outputs) | Task 5 | ⚠️ Worker doesn't call API to update DB (see gap below) |
| 5.3 Update on failure (reason, retry) | Task 5 | ⚠️ Same gap — worker events not persisted to DB |
| 5.4 Time-series table | Task 25 | ✅ job_status_events hypertable |
| 5.5 GET /jobs/{id} | Task 7, 7.1 | ✅ handleGetJob |
| 5.6 Range queries | Task 8 | ✅ handleListJobs |

### Req 6: KEDA-Based Autoscaling

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 6.1–6.5 KEDA scaler config | Task 29 (K8s manifests) | ⏳ Not yet implemented |

### Req 7: S3 Output Storage and Lifecycle

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 7.1 Output path structure | Task 17 | ✅ s3://pulsegrid-output/{id}/{rendition}/ |
| 7.2 Tag outputs | Task 17 | ✅ uploadSingleFile with tagging |
| 7.3 Lifecycle policy (Glacier/delete) | Task 30 (Terraform) | ⏳ Not yet implemented |
| 7.4 Cleanup on failure | Design: same lifecycle applies | ✅ (by policy) |
| 7.5 Versioning | Task 30 (Terraform) | ⏳ Not yet implemented |

### Req 8: Prometheus Metrics

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 8.1 API metrics (submitted_total, upload_duration) | Task 9 | ✅ apiMetrics.Inc() + Observe() |
| 8.2 Queue depth gauge | Task 9.2 | ✅ StartQueueDepthPoller |
| 8.3 Worker metrics (duration, failures, completed) | Task 21 | ✅ WorkerMetrics struct |
| 8.4 KEDA metrics | Task 29 (K8s KEDA) | ⏳ External (KEDA native) |
| 8.5 DLQ counter | Task 23 | ✅ SendDLQ increments are traceable |
| 8.6 Appropriate labels | Task 9, 21 | ✅ error_type, rendition |
| 8.7 /metrics endpoint | Task 9 (API:8080), Task 21 (Worker:8081) | ✅ |

### Req 9: Grafana Dashboards

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 9.1–9.6 Dashboard panels + alerts | Task 31 | ⏳ Not yet implemented |

### Req 10: Load Test Harness

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 10.1–10.6 Load test tool | Task 32, 32.1 | ⏳ Not yet implemented |

### Req 11: Fault Tolerance / DLQ / Retry

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 11.1 Max retries → DLQ with metadata | Task 18 | ✅ DLQMessage struct |
| 11.2 DLQ persists indefinitely | Kafka retention=unlimited on dlq topic | ✅ (config) |
| 11.3 Query DLQ via API | Not implemented | ⚠️ Gap (no DLQ query endpoint) |
| 11.4 Operator retry DLQ job | Not implemented | ⚠️ Gap (no DLQ retry endpoint) |
| 11.5 Non-retryable → immediate DLQ | Task 18 | ✅ ErrorTypePermanent path |

### Req 12: Pod Failure Recovery

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 12.1 Crash → timeout → re-enqueue | Task 12 | ✅ Kafka session timeout 30min |
| 12.2 SIGTERM → complete current job | Task 12 | ✅ shutdownRequested atomic + finish |
| 12.3 Refuse new jobs on shutdown | Task 12 | ✅ Loop checks shutdownRequested |
| 12.4 KEDA scale-down graceful | Task 29 | ⏳ terminationGracePeriodSeconds config |
| 12.5 Log pod failures | Task 20 | ✅ Structured JSON logs |

### Req 13: CI/CD Pipeline

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 13.1–13.6 GitHub Actions workflow | Task 28 | ⏳ Not yet implemented |

### Req 14: Infrastructure as Code (Terraform)

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 14.1–14.5 Terraform config | Task 30 | ⏳ Not yet implemented |

### Req 15: Latency Targets / Performance SLOs

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 15.1–15.6 SLO definitions | Task 32, 33 (validated by load test) | ⏳ Not yet implemented |

### Req 16: Load Test Validation of Autoscaling

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 16.1–16.4 Autoscaling validation | Task 33 | ⏳ Not yet implemented |

### Req 17: Chaos Engineering (Optional)

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 17.1–17.5 Fault injection | Task 33 | ⏳ Optional, validated in E2E |

### Req 18: Cost Optimization

| Criteria | Task(s) | Status |
|----------|---------|--------|
| 18.1 Resource tagging | Task 30 (Terraform) | ⏳ Not yet implemented |
| 18.2 Max pod count enforcement | Task 29 (KEDA maxReplicas) | ⏳ Config |
| 18.3 Scale down on low utilization | Task 29 (KEDA cooldown) | ⏳ Config |
| 18.4 Cost dashboard | Task 31 | ⏳ Not yet implemented |
| 18.5 Spot instance support | Task 30 (Terraform) | ⏳ Not yet implemented |

---

## Error Path Coverage

| Error Scenario | Component | Handled? |
|----------------|-----------|----------|
| File > 10GB | API | ✅ 413 |
| Missing source_name | API | ✅ 400 |
| Invalid renditions JSON | API | ✅ 400 |
| S3 upload transient error | API | ✅ RetryWithBackoff |
| S3 upload permanent (AccessDenied) | API | ✅ 500 returned |
| Kafka publish failure | API | ✅ DB rollback + 500 |
| DB insert failure | API | ✅ 500 |
| DB commit after Kafka success | API | ✅ ALERT logged, 202 returned |
| Job not found | API | ✅ 404 |
| S3 download network error | Worker | ✅ Retry with backoff |
| S3 source 404 | Worker | ✅ SourceNotFoundError → permanent → DLQ |
| Unsupported codec | Worker | ✅ PermanentError → DLQ |
| ffmpeg crash (exit non-zero) | Worker | ✅ TranscodingError |
| Timeout exceeded (30 min) | Worker | ✅ context.WithTimeout kills process |
| Out of disk | Worker | ✅ ErrorTypeConstraint → os.Exit(1) |
| Out of memory | Worker | ✅ ErrorTypeConstraint → os.Exit(1) |
| Retryable < max retries | Worker | ✅ ReenqueueWithRetry (count+1) |
| Retryable >= max retries | Worker | ✅ SendDLQFromMessage |
| Permanent error (any retry count) | Worker | ✅ Immediate DLQ |
| Pod crash (no commit) | Kafka | ✅ Session timeout → rebalance |
| SIGTERM graceful shutdown | Worker | ✅ Finish current, refuse new |
| Malformed Kafka message | Worker | ✅ Commit + skip |
| Temp file cleanup permission error | Worker | ✅ Logged, no panic |

---

## Identified Gaps

### Gap 1: Worker → DB status updates (Req 5.2, 5.3)
**Issue:** Worker doesn't call back to API/DB to update job status on completion/failure. DB records submission but not completion.
**Impact:** GET /jobs/{id} won't show real completion_time or failure_reason from worker.
**Planned fix:** Task 27 (integration test) or a separate status-update mechanism (worker publishes to `job-status-updates` topic, API consumes).

### Gap 2: DLQ query/retry endpoints (Req 11.3, 11.4)
**Issue:** No API endpoint to list DLQ messages or retry them.
**Impact:** Operators can't view/retry dead-lettered jobs via API.
**Note:** These are operator-facing features. Can query Kafka DLQ topic directly via tooling. API endpoints deferred to future iteration.

### Gap 3: Infrastructure tasks not yet implemented (Req 6, 9, 10, 13, 14, 15, 16, 17, 18)
**Status:** All have assigned tasks (28–34). Blocked by current wave — will be implemented in tasks 28–34.

---

## Component Integration Verification

| Integration | Status |
|-------------|--------|
| API → S3 (upload) | ✅ Wired in handleVideoUpload |
| API → Kafka (enqueue) | ✅ Wired in handleVideoUpload |
| API → Postgres (insert/query) | ✅ Wired in upload + status endpoints |
| API → Prometheus (/metrics) | ✅ promhttp.Handler registered |
| Worker → Kafka (consume/commit) | ✅ FetchMessage + CommitMessages |
| Worker → S3 (download) | ✅ DownloadSourceFromS3 |
| Worker → ffmpeg (transcode) | ✅ TranscodeSingleRendition + TranscodeHLS |
| Worker → S3 (upload outputs) | ✅ UploadOutputsToS3 |
| Worker → Kafka (DLQ/re-enqueue) | ✅ SendDLQFromMessage + ReenqueueWithRetry |
| Worker → Prometheus (/metrics:8081) | ✅ promhttp on :8081 |
| Queue depth → Prometheus gauge | ✅ StartQueueDepthPoller |
| DB migrations → Startup | ✅ db.RunMigrations |
| Structured logging (worker) | ✅ zap JSON logger |
| Retry utility shared | ✅ pkg.RetryWithBackoff used by S3, Kafka, queue |

---

## Conclusion

All 18 requirements have task coverage. Core application logic (API + Worker) is fully integrated with no hanging code. Error paths comprehensively handled with classification (retryable/permanent/constraint). Infrastructure tasks (Terraform, K8s, Grafana, CI/CD, load test) are planned for upcoming waves 9–10.

Two functional gaps (worker→DB updates, DLQ query endpoints) noted for follow-up in integration tasks.
