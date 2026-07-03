# Pulsegrid Design Document

## Overview

Pulsegrid is a distributed video transcoding platform designed to elastically scale video processing workloads on Kubernetes. The system architecture consists of four core components:

1. **API Server** (Go or Node.js): Accepts video uploads via multipart REST API, validates inputs, stores source videos in S3, and enqueues transcoding jobs
2. **Job Queue** (Kafka): Distributes transcoding work to worker pods with guaranteed delivery semantics and dead-letter queue support
3. **Worker Pods**: Containerized ffmpeg processors that consume jobs, perform transcoding, upload outputs to S3, and report status
4. **KEDA Scaler**: Kubernetes Event-Driven Autoscaling component that scales worker pod replicas based on queue depth

Supporting infrastructure includes Postgres/TimescaleDB for job state tracking, Prometheus for metrics collection, Grafana for visualization, and Terraform for Infrastructure as Code.

## Architecture

### System-Level Architecture Diagram

```
                                    ┌─────────────────────────────────────────┐
                                    │     Content Publishers (Clients)        │
                                    └───────────────┬──────────────────────────┘
                                                    │
                    ┌───────────────────────────────┼───────────────────────────┐
                    │ (multipart/form-data)         │                           │
                    │ POST /videos/upload           │ GET /jobs/{id}            │
                    ▼                               ▼                           │
        ┌──────────────────────────┐     ┌──────────────────────────┐         │
        │   API Server             │     │   Job Status Queries     │         │
        │ (Go or Node.js)          │     │   (from API Server)      │         │
        │ - Multipart handler      │     │                          │         │
        │ - File validation        │────▶│ Returns: {                          │
        │ - S3 upload orchestrator │     │   status, ETA, outputs,  │         │
        │ - Job enqueueing        │     │   URLs, errors}          │         │
        └──────────┬───────────────┘     └──────────────────────────┘         │
                   │                                                           │
                   │ (Job message)                                            │
                   ▼                                                           │
        ┌──────────────────────────┐                                         │
        │  Kafka (Job Queue)        │                                         │
        │ Topic: transcoding-jobs   │                                         │
        │ Partitions: 32            │                                         │
        │ Replication: 3            │                                         │
        │ DLQ Topic: transcoding-dlq│                                         │
        └──────────┬───────────────┘                                         │
                   │                                                           │
      ┌────────────┼────────────┬──────────────┐                             │
      │            │            │              │                             │
      ▼            ▼            ▼              ▼                             │
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                         │
│ Worker   │ │ Worker   │ │ Worker   │ │ Worker   │ ... (1-100 pods)       │
│  Pod 1   │ │  Pod 2   │ │  Pod 3   │ │  Pod n   │                         │
│ (ffmpeg) │ │ (ffmpeg) │ │ (ffmpeg) │ │ (ffmpeg) │                         │
└──────────┘ └──────────┘ └──────────┘ └──────────┘                         │
      │            │            │              │                             │
      └────────────┼────────────┴──────────────┘                             │
                   │                                                           │
                   │ (transcoded videos, manifests)                          │
                   ▼                                                           │
        ┌──────────────────────────┐                                         │
        │  S3 (Output Storage)      │                                         │
        │ s3://pulsegrid-output/    │                                         │
        │   {Job_ID}/               │                                         │
        │     {rendition}/{file}    │                                         │
        └──────────────────────────┘                                         │
                                                                              │
        ┌──────────────────────────┐     ┌──────────────────────────┐       │
        │  Postgres/TimescaleDB     │────▶│  Grafana Dashboard       │       │
        │ - jobs table              │     │ (visualizes metrics)     │       │
        │ - job_status_events       │     │                          │       │
        │ - Queries from API        │     └──────────────────────────┘       │
        └──────────────────────────┘                                         │
                   ▲                                                           │
                   │ (status updates)                                         │
                   │                                                           │
        ┌──────────┴───────────────┐                                         │
        │   Worker Pods & API      │                                         │
        │   (emit status)          │                                         │
        │                          │                                         │
        └──────────────────────────┘                                         │
                                                                              │
        ┌──────────────────────────┐                                         │
        │  KEDA ScaledObject       │                                         │
        │ Watches Kafka lag        │                                         │
        │ Scales Deployments       │                                         │
        │ Based on: ceil(queue/10) │                                         │
        └──────────────────────────┘                                         │
                                                                              │
        ┌──────────────────────────┐                                         │
        │  Prometheus              │                                         │
        │ (scrapes /metrics)       │◀─ Query responses to client ────────────┘
        │ (stores time-series)     │
        └──────────────────────────┘
```

### Component Interaction Flow

**Happy Path: Video Upload to Completion**

```
1. User uploads video to API_Server
   → POST /videos/upload with multipart/form-data
   → API validates file size, format, metadata
   → API generates UUID Job_ID (e.g., "550e8400-e29b-41d4-a716-446655440000")

2. API stores source video in S3
   → s3://pulsegrid-source/{Job_ID}/original.mp4
   → Tags: job_id, upload_time, source_name

3. API enqueues job to Kafka
   → Topic: transcoding-jobs
   → Partition: hash(Job_ID) mod 32
   → Message: {
       job_id: "550e8400-...",
       source_s3: "s3://pulsegrid-source/550e8400-.../ original.mp4",
       renditions: [{res: "720p", bitrate: "5M"}, {res: "480p", bitrate: "2.5M"}],
       output_prefix: "s3://pulsegrid-output/550e8400-...",
       retry_count: 0,
       timestamp: "2024-01-15T10:30:00Z"
     }

4. API records job metadata to Postgres
   → INSERT INTO jobs (job_id, status, submission_time, ...) 
     VALUES ("550e8400-...", "submitted", now(), ...)
   → INSERT INTO job_status_events (job_id, event_type, timestamp)
     VALUES ("550e8400-...", "submitted", now())

5. API returns HTTP 202 Accepted
   → Response: {
       job_id: "550e8400-...",
       status_uri: "/jobs/550e8400-...",
       estimated_wait_time_seconds: 120
     }

6. KEDA observes queue depth
   → Queries Kafka: queue_depth = 150 jobs
   → Calculates target: ceil(150 / 10) = 15 pods
   → Current replicas: 3, so KEDA scales to 15

7. Worker Pod (from pool) receives job
   → Polls Kafka topic: transcoding-jobs
   → Consumes message with lock (visibility timeout: 30 min)
   → Job_ID: "550e8400-..."

8. Worker Pod downloads source
   → Downloads from S3: s3://pulsegrid-source/550e8400-.../original.mp4
   → Stores locally: /tmp/550e8400-.../original.mp4 (100 GB)

9. Worker Pod transcodes
   → Invokes ffmpeg for each rendition in parallel:
     ffmpeg -i /tmp/.../original.mp4 \
       -c:v libx264 -b:v 5M -s 1280x720 /tmp/.../720p.mp4
   → Generates HLS chunks (6-second segments) + M3U8 playlist

10. Worker Pod generates manifest
    → Creates manifest.json with all output metadata
    → Lists files, sizes, durations, checksums

11. Worker Pod uploads outputs to S3
    → s3://pulsegrid-output/550e8400-.../720p/720p.mp4
    → s3://pulsegrid-output/550e8400-.../480p/480p.mp4
    → s3://pulsegrid-output/550e8400-.../hls/playlist.m3u8
    → s3://pulsegrid-output/550e8400-.../manifest.json
    → Tags: job_id, completion_time, rendition

12. Worker Pod acknowledges completion
    → Commits offset to Kafka (removes message from queue)
    → Publishes status update event

13. API records completion
    → UPDATE jobs SET status = "completed", completion_time = now()
    → INSERT INTO job_status_events (job_id, "completed", now())

14. Client polls job status
    → GET /jobs/550e8400-...
    → Response: {
        job_id: "...",
        status: "completed",
        submission_time: "...",
        completion_time: "...",
        output_files: [
          {path: "s3://.../720p.mp4", size: "..."},
          ...
        ],
        duration_seconds: 245
      }
```



## Components and Interfaces

### 1. API Server Component

**Technology Stack**: Go (preferred) or Node.js  
**Port**: 8080 (HTTP), 8081 (metrics)  
**Dependencies**: AWS SDK (S3), Kafka client, Postgres driver, Prometheus client

#### REST API Endpoints

##### POST /videos/upload
- **Purpose**: Accept video uploads and enqueue transcoding job
- **Content-Type**: multipart/form-data
- **Request Body**:
  ```
  Form Fields:
  - video (file, required): Video file (max 10GB by default)
  - source_name (string, required): Human-readable name for tracking
  - renditions (JSON, optional): Array of rendition specs
    Default: [{res: "720p", bitrate: "5M"}, {res: "480p", bitrate: "2.5M"}, {type: "hls"}]
  ```
- **Response**: HTTP 202 Accepted
  ```json
  {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status_uri": "/jobs/550e8400-e29b-41d4-a716-446655440000",
    "estimated_wait_time_seconds": 120,
    "submission_time": "2024-01-15T10:30:00Z"
  }
  ```
- **Error Responses**:
  - 400 Bad Request: Missing field, invalid JSON renditions, unsupported codec
  - 413 Payload Too Large: Video exceeds max size
  - 500 Internal Server Error: S3/Kafka/DB failure

**Implementation Details**:
- Stream upload to S3 using multipart upload API (no local disk buffering)
- Generate UUID v4 for Job_ID
- Validate rendition JSON schema
- Enqueue to Kafka within same transaction (transactional semantics)
- Record to Postgres with status="submitted"
- Return within 2 seconds

##### GET /jobs/{job_id}
- **Purpose**: Query job status and output locations
- **Response**: HTTP 200 OK
  ```json
  {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "completed|processing|submitted|failed",
    "submission_time": "2024-01-15T10:30:00Z",
    "completion_time": "2024-01-15T10:35:00Z",
    "estimated_completion_time": "2024-01-15T10:35:00Z",
    "retry_count": 0,
    "output_files": [
      {
        "rendition": "720p",
        "path": "s3://pulsegrid-output/550e8400-.../720p/720p.mp4",
        "size_bytes": 524288000,
        "duration_seconds": 300
      }
    ],
    "failure_reason": null
  }
  ```
- **Error Response**: HTTP 404 Not Found if job_id does not exist

##### GET /jobs (with filters)
- **Purpose**: Query multiple jobs by submission time range
- **Query Parameters**:
  - `submitted_after`: ISO 8601 timestamp (inclusive)
  - `submitted_before`: ISO 8601 timestamp (inclusive)
  - `status`: Comma-separated list (submitted, processing, completed, failed)
  - `limit`: Max 1000 results (default 100)
  - `offset`: Pagination offset (default 0)
- **Response**: HTTP 200 OK
  ```json
  {
    "jobs": [
      {
        "job_id": "...",
        "status": "completed",
        "submission_time": "...",
        "completion_time": "...",
        "duration_seconds": 245
      }
    ],
    "total": 5000,
    "limit": 100,
    "offset": 0
  }
  ```

##### GET /metrics
- **Purpose**: Expose Prometheus metrics
- **Response**: HTTP 200 OK (text/plain)
  ```
  # HELP pulsegrid_jobs_submitted_total Total jobs submitted
  # TYPE pulsegrid_jobs_submitted_total counter
  pulsegrid_jobs_submitted_total 15000

  # HELP pulsegrid_upload_duration_seconds Upload duration in seconds
  # TYPE pulsegrid_upload_duration_seconds histogram
  pulsegrid_upload_duration_seconds_bucket{le="0.1"} 100
  pulsegrid_upload_duration_seconds_bucket{le="1.0"} 5000
  pulsegrid_upload_duration_seconds_bucket{le="10.0"} 14900
  pulsegrid_upload_duration_seconds_bucket{le="+Inf"} 15000

  # HELP pulsegrid_queue_depth_jobs Current queue depth
  # TYPE pulsegrid_queue_depth_jobs gauge
  pulsegrid_queue_depth_jobs 147
  ```

#### Internal Business Logic

**Upload Handler**:
1. Parse multipart form-data
2. Validate file size (< 10GB)
3. Validate renditions JSON schema
4. Stream file to S3 with tagging
5. Generate UUID Job_ID
6. Create Kafka message with full spec
7. Publish to Kafka (serialized JSON)
8. Record metadata to Postgres
9. Return HTTP 202 with Job_ID

**Status Query Handler**:
1. Query Postgres for job metadata
2. Return current status from jobs table
3. If completed, fetch output locations from job_status_events or S3 listing
4. Estimate completion time based on historical data (if processing)

### 2. Job Queue (Kafka) Contract

**Broker Configuration**:
- Cluster: 3+ brokers (production)
- Replication Factor: 3 (production), 1 (dev)
- Min In-Sync Replicas: 2 (production)

**Topics**:

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| transcoding-jobs | 32 | 7 days | Main job queue |
| transcoding-dlq | 8 | Unlimited | Dead letter queue |
| job-status-updates | 16 | 1 day | Status change events |

**Job Message Schema** (JSON):
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_s3_uri": "s3://pulsegrid-source/550e8400-.../original.mp4",
  "source_file_size_bytes": 1073741824,
  "renditions": [
    {
      "id": "720p",
      "resolution": "1280x720",
      "video_codec": "libx264",
      "video_bitrate": "5M",
      "audio_codec": "aac",
      "audio_bitrate": "128k"
    },
    {
      "id": "480p",
      "resolution": "854x480",
      "video_codec": "libx264",
      "video_bitrate": "2.5M",
      "audio_codec": "aac",
      "audio_bitrate": "96k"
    },
    {
      "id": "hls",
      "type": "hls_segments",
      "segment_duration": 6,
      "base_resolution": "720p"
    }
  ],
  "output_s3_prefix": "s3://pulsegrid-output/550e8400-.../",
  "retry_count": 0,
  "max_retries": 3,
  "submitted_timestamp": "2024-01-15T10:30:00.000Z",
  "visibility_timeout_seconds": 1800
}
```

**Consumer Group**: `pulsegrid-workers` (all worker pods)  
**Partition Assignment**: Round-robin (Kafka default)

**Delivery Semantics**:
- At-least-once: Worker must acknowledge (commit offset) only after job completes
- Visibility Timeout: 30 minutes (if pod crashes, message re-delivered after timeout)
- Dead Letter Queue: After 3 failed retries, move to `transcoding-dlq` topic

**DLQ Message Schema** (extends Job Message):
```json
{
  "...": "...",
  "dlq_entry_timestamp": "2024-01-15T10:45:00.000Z",
  "failure_reason": "ffmpeg: unsupported codec - VP9",
  "failure_timestamp": "2024-01-15T10:45:00.000Z",
  "pod_id": "worker-pod-abc123",
  "stderr_snippet": "[libx264 @ 0x...] Unknown option 'invalid_param'"
}
```



### 3. Worker Pod Component

**Container Image**: `pulsegrid:worker-latest`  
**Base Image**: `ubuntu:22.04` or `alpine:latest` (ffmpeg-enabled)  
**Resource Requests**: 2 CPU, 4 GB RAM  
**Resource Limits**: 4 CPU, 8 GB RAM

**Entrypoint**:
```bash
#!/bin/bash
set -e

# Environment variables (injected by Kubernetes)
export KAFKA_BROKERS=${KAFKA_BROKERS:-kafka-0.kafka:9092,kafka-1.kafka:9092}
export CONSUMER_GROUP=pulsegrid-workers
export JOB_TOPIC=transcoding-jobs
export MAX_RETRIES=3
export VISIBILITY_TIMEOUT=1800

# Install signal handlers
trap 'handle_sigterm' SIGTERM

handle_sigterm() {
  echo "SIGTERM received, graceful shutdown..."
  # Signal main loop to exit after current job
  touch /tmp/shutdown
}

# Start job processing loop
/app/worker
```

**Core Processing Loop** (Pseudocode):
```
while true:
  if shutdown flag exists:
    break
  
  message = kafka_consumer.poll(timeout=5000ms)
  if message == null:
    continue
  
  try:
    job = parse_json(message.value)
    
    # Log start
    emit_metric("pulsegrid_job_started", 1)
    record_event("job_started", job.job_id)
    
    # Process
    result = process_job(job)
    
    # Upload outputs
    for file in result.output_files:
      upload_to_s3(file, tags={job_id, completion_time})
    
    # Record completion
    emit_metric("pulsegrid_transcode_duration_seconds", elapsed, labels={rendition})
    emit_metric("pulsegrid_job_completed", 1)
    record_event("job_completed", job.job_id)
    
    # Commit
    kafka_consumer.commit_async(message.offset)
    
  catch TranscodingError as e:
    if job.retry_count < MAX_RETRIES:
      # Increment retry count, re-enqueue
      job.retry_count += 1
      kafka_producer.send(message.key, encode_json(job))
    else:
      # Send to DLQ
      dlq_message = {**job, failure_reason: e.message, dlq_timestamp: now()}
      kafka_producer.send_dlq(encode_json(dlq_message))
    
    emit_metric("pulsegrid_transcode_failure", 1, labels={error_type: e.type})
    record_event("job_failed", job.job_id)
    kafka_consumer.commit_async(message.offset)
  
  catch ResourceConstraintError as e:
    # Non-retryable: out of disk or memory
    emit_metric("pulsegrid_pod_resource_constrained", 1)
    record_event("pod_resource_constrained", e.message)
    # Kill pod and let Kubernetes restart (visibility timeout will re-queue job)
    exit(1)
```

**Job Processing Function**:
```
process_job(job):
  temp_dir = create_temp_dir("/tmp/{job.job_id}")
  source_file = download_from_s3(job.source_s3_uri, temp_dir)
  
  results = {}
  
  for rendition in job.renditions:
    if rendition.type == "hls_segments":
      output = transcode_hls(source_file, rendition, temp_dir)
    else:
      output = transcode_single(source_file, rendition, temp_dir)
    
    results[rendition.id] = output
  
  manifest = generate_manifest(job, results)
  
  return {
    output_files: results + {manifest},
    success: true
  }

transcode_single(source, rendition, temp_dir):
  output_file = "{temp_dir}/{rendition.id}.mp4"
  
  ffmpeg_cmd = [
    "ffmpeg",
    "-i", source,
    "-c:v", rendition.video_codec,
    "-b:v", rendition.video_bitrate,
    "-s", rendition.resolution,
    "-c:a", rendition.audio_codec,
    "-b:a", rendition.audio_bitrate,
    "-y",  # Overwrite without asking
    output_file
  ]
  
  result = execute_with_timeout(ffmpeg_cmd, timeout=30min)
  
  if result.returncode != 0:
    raise TranscodingError(f"ffmpeg failed: {result.stderr}")
  
  return {
    rendition_id: rendition.id,
    file_path: output_file,
    file_size: get_file_size(output_file),
    duration: extract_duration_from_ffmpeg(result)
  }

transcode_hls(source, rendition, temp_dir):
  hls_dir = "{temp_dir}/hls"
  mkdir(hls_dir)
  
  ffmpeg_cmd = [
    "ffmpeg",
    "-i", source,
    "-c:v", "libx264",
    "-b:v", "5M",  # Use 720p bitrate
    "-s", "1280x720",
    "-c:a", "aac",
    "-b:a", "128k",
    "-f", "hls",
    "-hls_time", "6",           # 6-second segments
    "-hls_list_size", "0",      # Keep all segments in playlist
    f"{hls_dir}/playlist.m3u8"
  ]
  
  execute_with_timeout(ffmpeg_cmd, timeout=30min)
  
  return {
    rendition_id: "hls",
    playlist_path: f"{hls_dir}/playlist.m3u8",
    segments: glob(f"{hls_dir}/*.ts"),
    file_count: count_segments(hls_dir)
  }

generate_manifest(job, results):
  manifest = {
    job_id: job.job_id,
    source_file: job.source_s3_uri,
    output_files: results,
    generation_time: now(),
    worker_pod_id: env.HOSTNAME,
    ffmpeg_version: get_ffmpeg_version()
  }
  return manifest
```

**Error Handling**:
- Transient errors (network, S3 timeout): Retry (visibility timeout handles re-queue)
- Permanent errors (corrupted video, unsupported codec): Send to DLQ
- Resource constraints (disk full, OOM): Exit pod (Kubernetes restarts)

**Cleanup**:
- Delete `/tmp/{job_id}/` after job completes (success or failure)
- Close Kafka consumer connection gracefully on SIGTERM
- Log all errors with full context (job_id, pod_id, timestamp)

**Metrics Emission** (to Prometheus pushgateway or direct scrape):
- `pulsegrid_job_started` (counter)
- `pulsegrid_job_completed` (counter)
- `pulsegrid_transcode_duration_seconds` (histogram, labeled by rendition)
- `pulsegrid_transcode_failure` (counter, labeled by error_type)
- `pulsegrid_pod_resource_constrained` (counter)



## Data Models

### Postgres Database Schema

#### jobs Table
```sql
CREATE TABLE jobs (
  job_id UUID PRIMARY KEY,
  status VARCHAR(20) NOT NULL CHECK (status IN ('submitted', 'processing', 'completed', 'failed')),
  source_file_name VARCHAR(1024) NOT NULL,
  source_file_size_bytes BIGINT NOT NULL,
  source_s3_uri TEXT NOT NULL,
  output_s3_prefix TEXT NOT NULL,
  requested_renditions JSONB NOT NULL,  -- Array of rendition specs
  submission_time TIMESTAMP NOT NULL,
  processing_start_time TIMESTAMP,
  completion_time TIMESTAMP,
  failure_reason TEXT,
  retry_count INTEGER DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_jobs_status ON jobs (status);
CREATE INDEX idx_jobs_submission_time ON jobs (submission_time DESC);
CREATE INDEX idx_jobs_completion_time ON jobs (completion_time DESC);
```

#### job_status_events Table (TimescaleDB Hypertable)
```sql
CREATE TABLE job_status_events (
  job_id UUID NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  event_data JSONB,
  pod_id VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('job_status_events', 'event_timestamp', if_not_exists => TRUE);

CREATE INDEX idx_job_status_events_job_id ON job_status_events (job_id, event_timestamp DESC);
CREATE INDEX idx_job_status_events_timestamp ON job_status_events (event_timestamp DESC);
```

**Event Types**:
- `submitted`: Job enqueued to Kafka
- `processing_started`: Worker pod began transcoding
- `rendition_completed`: Single rendition done
- `completed`: All renditions done, outputs uploaded
- `failed`: Transcoding failed, moved to DLQ (or retrying)
- `pod_error`: Pod crash or resource constraint
- `status_event`: Any other status change

**Sample Event**:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "rendition_completed",
  "event_timestamp": "2024-01-15T10:32:15.123Z",
  "event_data": {
    "rendition_id": "720p",
    "output_s3_path": "s3://pulsegrid-output/550e8400-.../720p/720p.mp4",
    "output_file_size_bytes": 524288000,
    "duration_seconds": 135
  },
  "pod_id": "worker-pod-abc123"
}
```

#### Query Examples

**Job status lookup**:
```sql
SELECT job_id, status, submission_time, completion_time 
FROM jobs 
WHERE job_id = '550e8400-e29b-41d4-a716-446655440000';
```

**P99 latency over last hour**:
```sql
SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY 
  EXTRACT(EPOCH FROM (completion_time - submission_time)))
FROM jobs
WHERE completion_time > NOW() - INTERVAL '1 hour'
  AND status = 'completed';
```

**Failure rate by hour**:
```sql
SELECT 
  DATE_TRUNC('hour', submission_time) as hour,
  COUNT(*) as total_jobs,
  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
  ROUND(100.0 * SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM jobs
WHERE submission_time > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', submission_time)
ORDER BY hour DESC;
```

**Event history for a job**:
```sql
SELECT event_type, event_timestamp, event_data, pod_id
FROM job_status_events
WHERE job_id = '550e8400-e29b-41d4-a716-446655440000'
ORDER BY event_timestamp ASC;
```

### S3 Storage Structure

**Buckets**:
- `pulsegrid-source`: Source video uploads (input)
- `pulsegrid-output`: Transcoded videos (outputs)

**Source Bucket Path**:
```
s3://pulsegrid-source/{Job_ID}/original.mp4

Example:
s3://pulsegrid-source/550e8400-e29b-41d4-a716-446655440000/original.mp4
```

**Output Bucket Path**:
```
s3://pulsegrid-output/{Job_ID}/{rendition}/{filename}

Examples:
s3://pulsegrid-output/550e8400-.../720p/720p.mp4
s3://pulsegrid-output/550e8400-.../480p/480p.mp4
s3://pulsegrid-output/550e8400-.../hls/playlist.m3u8
s3://pulsegrid-output/550e8400-.../hls/segment-00000.ts
s3://pulsegrid-output/550e8400-.../hls/segment-00001.ts
s3://pulsegrid-output/550e8400-.../manifest.json
```

**S3 Object Tags** (applied by worker pods):
```
job_id = 550e8400-e29b-41d4-a716-446655440000
completion_time = 2024-01-15T10:35:00Z
rendition = 720p
source_name = marketing_video_2024.mp4
```

**Lifecycle Policies**:
- Source bucket: Delete after 30 days (after successful transcoding)
- Output bucket:
  - Transition to Glacier after 90 days
  - Expire (delete) after 365 days



## Error Handling

### Error Handling Strategy by Component

#### API Server Error Handling

| Error Scenario | HTTP Status | Response Body | Logging |
|---|---|---|---|
| File exceeds max size | 413 Payload Too Large | `{"error": "File exceeds 10GB limit", "size": 12000000000}` | WARN |
| Missing required field | 400 Bad Request | `{"error": "Missing field: renditions", "field": "renditions"}` | WARN |
| Invalid rendition JSON | 400 Bad Request | `{"error": "Invalid rendition JSON", "detail": "..."}` | WARN |
| S3 upload failure | 500 Internal Server Error | `{"error": "Failed to store video", "request_id": "..."}` | ERROR |
| Kafka publish failure | 500 Internal Server Error | `{"error": "Failed to queue job", "request_id": "..."}` | ERROR |
| Database insert failure | 500 Internal Server Error | `{"error": "Failed to record job", "request_id": "..."}` | ERROR |
| Job not found | 404 Not Found | `{"error": "Job not found", "job_id": "..."}` | INFO |

**Retry Policy for Transient Failures**:
- S3 upload: Exponential backoff (1s, 2s, 4s, 8s, 16s) - max 5 attempts
- Kafka publish: Exponential backoff (500ms, 1s, 2s, 4s, 8s) - max 5 attempts
- Database: Connection pool with retry logic - max 3 attempts

**Error Response Format**:
```json
{
  "error": "Human-readable error message",
  "error_code": "INTERNAL_ERROR|VALIDATION_ERROR|NOT_FOUND|SERVICE_UNAVAILABLE",
  "request_id": "uuid-for-tracing",
  "timestamp": "2024-01-15T10:30:00Z",
  "detail": "Additional context if available"
}
```

#### Worker Pod Error Handling

| Error Scenario | Action | Retry? | DLQ? | Logging |
|---|---|---|---|---|
| Network error downloading source | Log, sleep, retry | Yes (via visibility timeout) | No | ERROR |
| S3 source file not found | Log, fail permanently | No | Yes | ERROR |
| Unsupported video codec | Log, fail permanently | No | Yes | ERROR |
| ffmpeg crash/exit non-zero | Log stderr, fail | Yes | After max retries | ERROR |
| Out of disk space | Log, exit pod | Yes (restart pod) | No | ERROR |
| Out of memory | Exit pod | Yes (restart pod) | No | ERROR |
| Kafka commit failure | Log, mark locally as processed | No (retry via visibility) | No | ERROR |
| S3 upload failure (transient) | Exponential backoff, retry | Yes | After max retries | ERROR |
| S3 upload failure (permanent) | Fail job, move to DLQ | No | Yes | ERROR |

**Graceful Degradation**:
- If only one rendition fails, continue with others if possible (best-effort)
- If all renditions fail, fail the entire job
- Temporary files cleaned up regardless of outcome

### Failure Mode Analysis

1. **Pod crashes during transcoding**: Kafka visibility timeout (30 min) expires, job re-queued to different pod
2. **S3 temporary unavailability**: Worker retries with exponential backoff; if persistent, after timeout job goes to DLQ
3. **Kafka broker failure**: API Server buffering disabled; errors returned immediately to client; client can retry
4. **Database connection loss**: Connection pool handles reconnection; transient queries may fail (client can retry)
5. **KEDA unable to scale**: Existing pods continue processing; queue depth grows; manual intervention required
6. **ffmpeg corrupted/missing**: Worker pod startup fails (OOMKilled); Kubernetes restarts pod; may fail pod health checks



## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

After analyzing the requirements, the following acceptance criteria are suitable for property-based testing:

1. **Job ID generation**: Verifying uniqueness and UUID v4 format
2. **Kafka message schema**: Verifying message structure conformance
3. **Retry logic**: Verifying retry count incrementation and re-queue behavior
4. **DLQ handling**: Verifying failed jobs are moved to DLQ with metadata
5. **Rendition processing**: Verifying all requested renditions are produced
6. **Manifest generation**: Verifying manifest JSON schema and completeness
7. **Database persistence**: Verifying job metadata is persisted and queryable
8. **Metrics emission**: Verifying metrics are emitted with correct labels
9. **Temp file cleanup**: Verifying temporary files are deleted after processing
10. **Error logging**: Verifying logs contain required context fields

Note: Many requirements test external service behavior (AWS S3, Kafka broker semantics, KEDA scaling, Kubernetes pod management) which are not suitable for property-based testing of our application logic. These require integration tests. PBT focuses on our code's logic layers.

### Property 1: Job ID Uniqueness and Format

*For any sequence of upload requests, all generated Job IDs SHALL be unique UUIDs matching the RFC 4122 v4 specification.*

**Validates: Requirements 1.4**

**Test Implementation**: 
- Generate 1000 random upload requests
- Collect all Job IDs returned
- Verify no duplicates (set size == list size)
- Verify each ID matches UUID v4 regex: `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`

### Property 2: Kafka Job Message Schema Compliance

*For any valid upload request with any rendition specification, the resulting Kafka message SHALL contain all required fields (job_id, source_s3_uri, renditions, output_s3_prefix, retry_count) with correct types.*

**Validates: Requirements 2.1, 1.6**

**Test Implementation**:
- Generate random rendition specifications (varying in count, resolution, codec, bitrate)
- Submit uploads via API
- Consume Kafka messages
- Verify message JSON schema:
  - job_id: string (UUID)
  - source_s3_uri: string (s3:// URL)
  - renditions: array of objects with id, resolution, codec, bitrate
  - output_s3_prefix: string (s3:// prefix)
  - retry_count: integer (>= 0)

### Property 3: Retry Count Increment on Failure

*For any job message with retry_count < max_retries (3), when processing fails, the retry_count SHALL be incremented by 1 and the message SHALL be re-enqueued to the same Kafka topic.*

**Validates: Requirements 2.4**

**Test Implementation**:
- Generate jobs with retry_count in range [0, 2]
- Simulate processing failure (mock ffmpeg error)
- Verify Kafka message is re-enqueued with retry_count + 1
- Verify message offset is committed only after re-queue succeeds

### Property 4: Dead Letter Queue Entry on Max Retries

*For any job message with retry_count >= max_retries (3), when processing fails, the job SHALL be moved to the Dead Letter Queue with metadata fields: job_id, failure_reason, dlq_entry_timestamp, and final retry_count.*

**Validates: Requirements 2.5, 11.1**

**Test Implementation**:
- Generate jobs with retry_count = 3 (max retries reached)
- Simulate processing failure
- Verify message is published to transcoding-dlq topic
- Verify DLQ message contains all required fields
- Verify DLQ message has valid timestamp (ISO 8601)
- Verify failure_reason is non-empty string

### Property 5: All Requested Renditions Are Produced

*For any transcoding request specifying a set of renditions, the output SHALL contain all requested renditions (round-trip: input renditions == output renditions).*

**Validates: Requirements 4.2**

**Test Implementation**:
- Generate random rendition lists (720p, 480p, HLS, custom combos)
- Mock ffmpeg to produce expected output files
- Run transcoding
- Verify output contains all requested rendition files
- Verify manifest lists all input renditions in output

### Property 6: Manifest Generation Schema

*For any completed transcoding with any number of output renditions, the generated manifest.json SHALL be valid JSON containing: job_id, source_file, output_files array, generation_time, worker_pod_id, and ffmpeg_version.*

**Validates: Requirements 4.4**

**Test Implementation**:
- Generate random transcoding results with varying rendition counts
- Generate manifest JSON
- Verify manifest parses as valid JSON
- Verify all required fields are present
- Verify output_files array matches actual output files
- Verify generation_time is valid ISO 8601 timestamp

### Property 7: Database Persistence and Round-Trip

*For any uploaded job, the job metadata SHALL be recorded to Postgres and a subsequent query SHALL return matching status, submission_time, source_file_name, and requested_renditions.*

**Validates: Requirements 5.1, 5.5**

**Test Implementation**:
- Generate random upload requests with various rendition specs
- Submit uploads (mocked S3, real/mocked Postgres)
- Query database for each job_id
- Verify returned record matches submitted metadata
- Verify no data loss or corruption

### Property 8: Metrics Emission with Correct Labels

*For any system event (job_submitted, job_completed, transcode_failure), the corresponding Prometheus metrics SHALL be emitted with all required labels: job_id (for sensitive metrics), rendition (for per-format breakdown), error_type (for failures).*

**Validates: Requirements 8.1-8.7**

**Test Implementation**:
- Generate random events (submissions, completions, failures with various error types)
- Scrape /metrics endpoint
- Parse Prometheus text format
- Verify metric counters/gauges are incremented correctly
- Verify all required labels are present for each metric
- Verify label values are non-empty

### Property 9: Temporary File Cleanup

*For any processed job (successful or failed), all temporary files in /tmp/{job_id}/ SHALL be deleted after job processing completes.*

**Validates: Requirements 3.6**

**Test Implementation**:
- Generate random jobs
- Mock Kafka and S3
- Run job processing (success and failure cases)
- Verify /tmp/{job_id}/ directory does not exist after processing
- Verify no orphaned files remain

### Property 10: Error Logging Context

*For any transcoding error, the error log entry SHALL contain all context fields: job_id, pod_id, timestamp, and error_message.*

**Validates: Requirements 3.5, 12.2**

**Test Implementation**:
- Simulate various transcoding errors (ffmpeg failure, network error, resource constraint)
- Capture log output
- Parse log entries (structured JSON logging)
- Verify each error log contains: job_id, pod_id (HOSTNAME), timestamp (ISO 8601), error_message

## Testing Strategy

### Test Categories and Approach

Given the distributed nature of Pulsegrid, testing involves:

1. **Property-Based Tests** (High Coverage): Test core business logic with generated inputs
   - Job ID generation (Property 1)
   - Kafka message schema (Property 2)
   - Retry logic (Property 3)
   - Rendition processing (Property 5)
   - Database round-trips (Property 7)
   - Metrics emission (Property 8)
   - Error logging (Property 10)

2. **Integration Tests** (Coverage of External Services):
   - API endpoint tests with real HTTP client (mocked S3, Kafka, DB)
   - Kafka producer/consumer tests (message delivery, acknowledgment)
   - Database schema and query tests (Postgres-specific)
   - S3 lifecycle policy verification (AWS integration)
   - KEDA scaling trigger tests (Kubernetes integration)

3. **End-to-End Tests** (Full System Validation):
   - Upload → queue → process → complete flow
   - Failure → retry → success flow
   - Failure → DLQ flow
   - Load test harness validation (autoscaling, latency percentiles, SLO verification)

4. **Chaos Engineering Tests** (Resilience Validation):
   - Pod crash during transcoding (verify re-queue and completion)
   - S3 temporary failure (verify retry and eventual success)
   - Kafka broker temporary unavailability (verify error handling)

### Property Test Configuration

Each property-based test SHALL:
- Run minimum **100 iterations** (due to randomization)
- Use generators for: file sizes, rendition specifications, retry counts, timestamps
- Mock external services (S3, Kafka, Postgres) for isolation
- Include assertion of universal property
- Be tagged with: `Feature: pulsegrid, Property {number}: {property_text}`

**Example: Property 1 (Job ID Uniqueness)**
```go
// Feature: pulsegrid, Property 1: Job IDs are unique and RFC 4122 v4 compliant
func TestJobIDUniquenessAndFormat(t *testing.T) {
  property.ForAll(t, gen.SliceOf(genUploadRequest()),
    func(requests []UploadRequest) bool {
      ids := make(map[string]bool)
      for _, req := range requests {
        jobID := GenerateJobID() // Call our UUID generator
        
        // Check uniqueness
        if ids[jobID] {
          return false
        }
        ids[jobID] = true
        
        // Check RFC 4122 v4 format
        if !isValidUUIDv4(jobID) {
          return false
        }
      }
      return true
    })
}
```

### Unit Test Coverage

- API endpoint request/response handling: HTTP status codes, JSON marshaling
- Input validation: file size limits, rendition schema validation, metadata validation
- Error response formatting: correct error codes, messages, request IDs
- Retry logic edge cases: boundary conditions (retry_count = max_retries - 1, max_retries, max_retries + 1)
- Manifest generation with edge cases: zero renditions, very large rendition counts
- Temp file cleanup: success paths, failure paths, partial failures

### Integration Test Coverage

- API server with mocked S3, Kafka, Postgres
- Worker pod with mocked Kafka, S3, file system
- Database schema: table creation, indexes, queries returning correct results
- Kafka producer/consumer: message ordering, offset management, DLQ behavior
- S3 path structure: correct prefixes, tagging, lifecycle policies

### Kubernetes/Infrastructure Tests

- Deployment manifests: correct resource requests, labels, selectors, environment variables
- KEDA ScaledObject: correct Kafka topic lag metric, scaling formula, min/max bounds
- Graceful shutdown: SIGTERM handling, job completion before exit, no orphaned processes
- Pod restart behavior: Jobs recovered after pod crash
- Health checks: liveness/readiness probes functioning correctly

### Load Test Harness Tests

- Configuration parsing: accepts valid JSON configs, rejects invalid configs
- Job submission: generates correct HTTP requests, collects Job IDs
- Polling loop: queries status repeatedly, records timestamps correctly
- Report generation: outputs valid JSON, includes required metrics (p50, p95, p99)
- SLO validation: correctly identifies violations (e.g., p99 > 30 minutes), produces markdown summary

### Performance SLO Targets (from Requirements)

These will be verified by load testing, not unit/property tests:
- **p50 latency**: 5 minutes for 1GB video into 3 renditions
- **p99 latency**: 30 minutes for 1GB video into 3 renditions
- **Success rate**: 99.5% (< 0.5% DLQ failure rate)
- **Scale-up time**: 60 seconds from job submission to scaled pod ready
- **API response time**: 2 seconds for upload endpoint
- **Scale-down time**: 10 minutes from queue empty to pods reduced



## Kubernetes Manifests

### API Server Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pulsegrid-api
  namespace: pulsegrid
  labels:
    app: pulsegrid-api
    version: v1
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pulsegrid-api
  template:
    metadata:
      labels:
        app: pulsegrid-api
    spec:
      containers:
      - name: api
        image: pulsegrid:api-latest
        imagePullPolicy: IfNotPresent
        ports:
        - name: http
          containerPort: 8080
        - name: metrics
          containerPort: 8081
        env:
        - name: PORT
          value: "8080"
        - name: KAFKA_BROKERS
          value: kafka-0.kafka.pulsegrid.svc.cluster.local:9092,kafka-1.kafka.pulsegrid.svc.cluster.local:9092
        - name: DB_HOST
          value: postgres.pulsegrid.svc.cluster.local
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: password
        - name: S3_BUCKET_SOURCE
          value: pulsegrid-source
        - name: S3_BUCKET_OUTPUT
          value: pulsegrid-output
        - name: AWS_REGION
          value: us-east-1
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 1000m
            memory: 1Gi
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
      serviceAccountName: pulsegrid-api
---
apiVersion: v1
kind: Service
metadata:
  name: pulsegrid-api
  namespace: pulsegrid
spec:
  selector:
    app: pulsegrid-api
  ports:
  - name: http
    port: 80
    targetPort: 8080
  - name: metrics
    port: 8081
    targetPort: 8081
  type: LoadBalancer
```

### Worker Pod Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pulsegrid-worker
  namespace: pulsegrid
spec:
  replicas: 1  # Will be scaled by KEDA
  selector:
    matchLabels:
      app: pulsegrid-worker
  template:
    metadata:
      labels:
        app: pulsegrid-worker
    spec:
      containers:
      - name: worker
        image: pulsegrid:worker-latest
        imagePullPolicy: IfNotPresent
        ports:
        - name: metrics
          containerPort: 8081
        env:
        - name: KAFKA_BROKERS
          value: kafka-0.kafka.pulsegrid.svc.cluster.local:9092,kafka-1.kafka.pulsegrid.svc.cluster.local:9092
        - name: CONSUMER_GROUP
          value: pulsegrid-workers
        - name: JOB_TOPIC
          value: transcoding-jobs
        - name: DLQ_TOPIC
          value: transcoding-dlq
        - name: MAX_RETRIES
          value: "3"
        - name: VISIBILITY_TIMEOUT
          value: "1800"
        - name: AWS_REGION
          value: us-east-1
        - name: S3_BUCKET_OUTPUT
          value: pulsegrid-output
        - name: S3_BUCKET_SOURCE
          value: pulsegrid-source
        - name: DB_HOST
          value: postgres.pulsegrid.svc.cluster.local
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: password
        resources:
          requests:
            cpu: 2
            memory: 4Gi
          limits:
            cpu: 4
            memory: 8Gi
        lifecycle:
          preStop:
            exec:
              command: ["/app/graceful-shutdown.sh"]
        livenessProbe:
          exec:
            command:
            - /app/health-check.sh
            - liveness
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          exec:
            command:
            - /app/health-check.sh
            - readiness
          initialDelaySeconds: 10
          periodSeconds: 10
      serviceAccountName: pulsegrid-worker
      terminationGracePeriodSeconds: 300
```

### KEDA ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: pulsegrid-worker-scaler
  namespace: pulsegrid
spec:
  scaleTargetRef:
    name: pulsegrid-worker
  minReplicaCount: 1
  maxReplicaCount: 100
  cooldownPeriod: 300
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka-0.kafka.pulsegrid.svc.cluster.local:9092,kafka-1.kafka.pulsegrid.svc.cluster.local:9092
      consumerGroup: pulsegrid-workers
      topic: transcoding-jobs
      lagThreshold: "10"
      offsetResetPolicy: "latest"
    authModes:
      - "none"
```

**Scaling Formula**: KEDA will scale based on Kafka consumer lag.
- When lag > 10 messages per pod, scale up
- Formula: `ceil(lag / 10)`
- Example: lag = 150 → target replicas = 15

### ConfigMap and Secrets

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pulsegrid-config
  namespace: pulsegrid
data:
  max_file_size_gb: "10"
  default_renditions: |
    [
      {"id": "720p", "resolution": "1280x720", "bitrate": "5M"},
      {"id": "480p", "resolution": "854x480", "bitrate": "2.5M"},
      {"id": "hls", "type": "hls_segments", "segment_duration": 6}
    ]
  max_retries: "3"
  visibility_timeout_seconds: "1800"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: pulsegrid
type: Opaque
data:
  username: cG9zdGdyZXM=  # base64: postgres
  password: <base64-encoded-password>
```

### Service Accounts and RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pulsegrid-api
  namespace: pulsegrid
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pulsegrid-worker
  namespace: pulsegrid
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pulsegrid-worker
  namespace: pulsegrid
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: pulsegrid-worker
  namespace: pulsegrid
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: pulsegrid-worker
subjects:
- kind: ServiceAccount
  name: pulsegrid-worker
```



## Infrastructure as Code (Terraform)

### Main Infrastructure Module

```hcl
# main.tf: Core infrastructure resources

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
  
  backend "s3" {
    bucket         = "pulsegrid-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
}

# EKS Cluster
resource "aws_eks_cluster" "pulsegrid" {
  name            = "pulsegrid-${var.environment}"
  role_arn        = aws_iam_role.eks_cluster_role.arn
  version         = "1.28"
  
  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    security_groups         = [aws_security_group.eks_cluster.id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }
  
  tags = local.common_tags
}

# EKS Node Group (Auto Scaling)
resource "aws_eks_node_group" "pulsegrid_workers" {
  cluster_name    = aws_eks_cluster.pulsegrid.name
  node_group_name = "pulsegrid-workers"
  node_role_arn   = aws_iam_role.node_role.arn
  subnet_ids      = aws_subnet.private[*].id
  
  scaling_config {
    min_size     = 1
    max_size     = 100
    desired_size = 3
  }
  
  instance_types = ["t3.2xlarge"]  # 8 CPU, 32 GB RAM per node
  disk_size      = 100
  
  tags = merge(local.common_tags, {
    Name = "pulsegrid-worker-nodes"
  })
}

# S3 Buckets
resource "aws_s3_bucket" "pulsegrid_source" {
  bucket = "pulsegrid-source-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "pulsegrid_output" {
  bucket = "pulsegrid-output-${var.environment}"
  tags   = local.common_tags
}

# S3 Lifecycle Policies
resource "aws_s3_bucket_lifecycle_configuration" "source_lifecycle" {
  bucket = aws_s3_bucket.pulsegrid_source.id
  
  rule {
    id     = "delete-after-30-days"
    status = "Enabled"
    
    expiration {
      days = 30
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "output_lifecycle" {
  bucket = aws_s3_bucket.pulsegrid_output.id
  
  rule {
    id     = "glacier-after-90-days"
    status = "Enabled"
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
  
  rule {
    id     = "delete-after-365-days"
    status = "Enabled"
    
    expiration {
      days = 365
    }
  }
}

# RDS Postgres Database
resource "aws_db_instance" "pulsegrid" {
  identifier     = "pulsegrid-${var.environment}"
  engine         = "postgres"
  engine_version = "15.3"
  instance_class = "db.r6i.xlarge"  # Multi-AZ capable
  
  allocated_storage = 100
  storage_type      = "gp3"
  
  db_name  = "pulsegrid"
  username = "postgres"
  password = random_password.db_password.result
  
  multi_az            = true
  publicly_accessible = false
  
  skip_final_snapshot       = false
  final_snapshot_identifier = "pulsegrid-${var.environment}-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  
  backup_retention_period = 30
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"
  
  tags = merge(local.common_tags, {
    Name = "pulsegrid-database"
  })
}

# IAM Roles for EKS Nodes
resource "aws_iam_role" "node_role" {
  name = "pulsegrid-node-role-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.node_role.name
}

# IAM Policy for S3 and Secrets Manager Access
resource "aws_iam_policy" "pulsegrid_worker_policy" {
  name = "pulsegrid-worker-policy-${var.environment}"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutObjectTagging",
          "s3:GetObjectTagging"
        ]
        Resource = [
          "${aws_s3_bucket.pulsegrid_source.arn}/*",
          "${aws_s3_bucket.pulsegrid_output.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.pulsegrid_source.arn,
          aws_s3_bucket.pulsegrid_output.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:pulsegrid/*"
      }
    ]
  })
}

# VPC and Networking
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = merge(local.common_tags, {
    Name = "pulsegrid-vpc"
  })
}

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  tags = merge(local.common_tags, {
    Name = "pulsegrid-private-subnet-${count.index + 1}"
  })
}

resource "aws_security_group" "eks_cluster" {
  name   = "pulsegrid-eks-cluster-sg-${var.environment}"
  vpc_id = aws_vpc.main.id
  
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Restrict in production
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  common_tags = {
    Project     = "pulsegrid"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}
```

### Variables and Outputs

```hcl
# variables.tf

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "eks_cluster_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "node_instance_type" {
  description = "EC2 instance type for worker nodes"
  type        = string
  default     = "t3.2xlarge"
}

variable "min_nodes" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "max_nodes" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 100
}

# outputs.tf

output "eks_cluster_name" {
  value = aws_eks_cluster.pulsegrid.name
}

output "eks_cluster_endpoint" {
  value = aws_eks_cluster.pulsegrid.endpoint
}

output "s3_source_bucket" {
  value = aws_s3_bucket.pulsegrid_source.id
}

output "s3_output_bucket" {
  value = aws_s3_bucket.pulsegrid_output.id
}

output "rds_endpoint" {
  value = aws_db_instance.pulsegrid.endpoint
}

output "rds_database_name" {
  value = aws_db_instance.pulsegrid.db_name
}
```

### Deployment Commands

```bash
# Initialize and plan
terraform init
terraform plan -var="environment=staging" -out=tfplan

# Apply
terraform apply tfplan

# Destroy (if needed)
terraform destroy -var="environment=staging"
```



## Observability and Monitoring

### Prometheus Metrics

#### API Server Metrics

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `pulsegrid_jobs_submitted_total` | Counter | - | Total jobs submitted to API |
| `pulsegrid_upload_duration_seconds` | Histogram | `le` (buckets) | Upload processing time |
| `pulsegrid_upload_size_bytes` | Histogram | - | Uploaded file size distribution |
| `pulsegrid_queue_depth_jobs` | Gauge | - | Current Kafka queue depth |
| `pulsegrid_queue_depth_dlq_jobs` | Gauge | - | Current DLQ depth |
| `pulsegrid_db_query_duration_seconds` | Histogram | `query_type` | Database query latency |

#### Worker Pod Metrics

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `pulsegrid_transcode_duration_seconds` | Histogram | `rendition` | Transcoding time per rendition |
| `pulsegrid_transcode_failures_total` | Counter | `error_type` | Failed transcoding attempts |
| `pulsegrid_job_completed_total` | Counter | `rendition` | Successfully completed jobs |
| `pulsegrid_pod_resource_constrained_total` | Counter | `constraint_type` (disk/memory) | Pod resource constraint events |
| `pulsegrid_pod_ffmpeg_invocations_total` | Counter | - | Number of ffmpeg invocations |

#### KEDA Metrics

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `pulsegrid_worker_pods_current` | Gauge | - | Current number of worker pod replicas |
| `pulsegrid_worker_pods_target` | Gauge | - | Target replicas calculated by KEDA |
| `pulsegrid_scaling_events_total` | Counter | `direction` (up/down) | Total scaling events |

### Grafana Dashboard Design

**Main Dashboard: Pulsegrid System Overview**

| Panel | Query | Visualization | Refresh |
|---|---|---|---|
| Queue Depth | `pulsegrid_queue_depth_jobs` | Gauge (0-1000) | 15s |
| Worker Pod Count | `pulsegrid_worker_pods_current` | Gauge (0-100) | 15s |
| Target Pod Count | `pulsegrid_worker_pods_target` | Gauge (0-100) | 15s |
| P50 Latency | `histogram_quantile(0.50, rate(pulsegrid_transcode_duration_seconds_bucket[5m]))` | Graph | 1m |
| P95 Latency | `histogram_quantile(0.95, rate(pulsegrid_transcode_duration_seconds_bucket[5m]))` | Graph | 1m |
| P99 Latency | `histogram_quantile(0.99, rate(pulsegrid_transcode_duration_seconds_bucket[5m]))` | Graph | 1m |
| Failure Rate | `rate(pulsegrid_transcode_failures_total[5m])` | Percentage | 1m |
| Failure Reason Breakdown | `sum by (error_type) (rate(pulsegrid_transcode_failures_total[5m]))` | Pie chart | 1m |
| Jobs Completed/min | `rate(pulsegrid_job_completed_total[1m])` | Graph | 1m |
| Est. Time to Empty | `pulsegrid_queue_depth_jobs / (rate(pulsegrid_job_completed_total[1m]) + 0.001)` | Gauge | 1m |
| DLQ Jobs | `pulsegrid_queue_depth_dlq_jobs` | Gauge | 1m |
| Per-Rendition Latency | `histogram_quantile(0.95, sum by (rendition) (rate(pulsegrid_transcode_duration_seconds_bucket[5m])))` | Graph | 1m |
| Scaling Events | `rate(pulsegrid_scaling_events_total[5m])` | Graph | 1m |

### Alert Rules

```yaml
groups:
- name: pulsegrid
  interval: 30s
  rules:
  - alert: HighQueueDepth
    expr: pulsegrid_queue_depth_jobs > 100
    for: 5m
    annotations:
      summary: "Queue depth exceeds 100 jobs"
      description: "Queue depth is {{ $value }} jobs"
  
  - alert: HighFailureRate
    expr: rate(pulsegrid_transcode_failures_total[5m]) > 0.05
    for: 5m
    annotations:
      summary: "Failure rate exceeds 5%"
      description: "Failure rate is {{ $value }}"
  
  - alert: HighP99Latency
    expr: histogram_quantile(0.99, rate(pulsegrid_transcode_duration_seconds_bucket[5m])) > 1800
    for: 10m
    annotations:
      summary: "P99 latency exceeds 30 minutes"
      description: "P99 latency is {{ $value }} seconds"
  
  - alert: DLQBacklog
    expr: pulsegrid_queue_depth_dlq_jobs > 10
    for: 5m
    annotations:
      summary: "Dead Letter Queue has backlog"
      description: "DLQ depth is {{ $value }} jobs"
  
  - alert: PodResourceConstrained
    expr: rate(pulsegrid_pod_resource_constrained_total[5m]) > 0
    for: 1m
    annotations:
      summary: "Worker pod resource constrained"
      description: "{{ $value }} constraints detected per second"
  
  - alert: APIServerDown
    expr: up{job="pulsegrid-api"} == 0
    for: 1m
    annotations:
      summary: "API Server is down"
```

### Logging Strategy

**Structured Logging** (JSON format):
```json
{
  "timestamp": "2024-01-15T10:35:00.123Z",
  "level": "ERROR",
  "service": "worker-pod",
  "pod_id": "worker-pod-abc123",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "transcode_failure",
  "message": "ffmpeg exited with non-zero status",
  "ffmpeg_exitcode": 1,
  "ffmpeg_stderr": "[libx264 @ 0x...] Unknown option 'invalid_param'",
  "retry_count": 2,
  "request_id": "req-12345"
}
```

**Log Aggregation**:
- Collect logs from API Server, Worker Pods, KEDA via sidecar or stdout
- Send to CloudWatch Logs, DataDog, or similar
- Retention: 30 days (configurable)
- Searchable by: job_id, pod_id, event_type, timestamp



## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/deploy.yml

name: Build and Deploy

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
      kafka:
        image: confluentinc/cp-kafka:7.5.0
        env:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    
    - name: Run unit tests
      run: |
        go test -v -race -coverprofile=coverage.out ./...
        go tool cover -html=coverage.out -o coverage.html
    
    - name: Run integration tests
      run: |
        go test -v -tags=integration ./tests/integration/...
      env:
        DATABASE_URL: postgres://postgres:postgres@localhost:5432/pulsegrid_test
        KAFKA_BROKERS: localhost:9092
    
    - name: Run linter
      uses: golangci/golangci-lint-action@v3
      with:
        version: latest
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        files: ./coverage.out

  build:
    needs: test
    runs-on: ubuntu-latest
    if: github.event_name == 'push'
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v2
    
    - name: Login to ECR
      uses: aws-actions/amazon-ecr-login@v1
      with:
        registries: ${{ secrets.AWS_ACCOUNT_ID }}
    
    - name: Build and push API image
      uses: docker/build-push-action@v4
      with:
        context: ./cmd/api
        push: true
        tags: |
          ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:api-${{ github.sha }}
          ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:api-latest
        cache-from: type=gha
        cache-to: type=gha,mode=max
    
    - name: Build and push worker image
      uses: docker/build-push-action@v4
      with:
        context: ./cmd/worker
        push: true
        tags: |
          ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:worker-${{ github.sha }}
          ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:worker-latest
        cache-from: type=gha
        cache-to: type=gha,mode=max

  deploy-staging:
    needs: build
    runs-on: ubuntu-latest
    if: github.event_name == 'push'
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Configure kubectl
      run: |
        aws eks update-kubeconfig --region us-east-1 --name pulsegrid-staging
      env:
        AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
        AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    
    - name: Update image tags
      run: |
        sed -i "s|image: pulsegrid:api-latest|image: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:api-${{ github.sha }}|g" k8s/api-deployment.yaml
        sed -i "s|image: pulsegrid:worker-latest|image: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:worker-${{ github.sha }}|g" k8s/worker-deployment.yaml
    
    - name: Deploy to staging
      run: |
        kubectl apply -f k8s/
        kubectl set image deployment/pulsegrid-api api=${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:api-${{ github.sha }} -n pulsegrid
        kubectl set image deployment/pulsegrid-worker worker=${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:worker-${{ github.sha }} -n pulsegrid
        kubectl rollout status deployment/pulsegrid-api -n pulsegrid
        kubectl rollout status deployment/pulsegrid-worker -n pulsegrid
    
    - name: Run smoke tests
      run: |
        kubectl port-forward svc/pulsegrid-api 80:80 -n pulsegrid &
        sleep 5
        curl -f http://localhost/health || exit 1
        ./tests/smoke/upload_test.sh
    
    - name: Run load test
      run: |
        go run ./cmd/load-test-harness \
          --config tests/load-test/staging-config.json \
          --api-endpoint http://localhost \
          --output staging-load-test-report.json
    
    - name: Check load test results
      run: |
        ./scripts/validate_slos.sh staging-load-test-report.json
    
    - name: Upload load test report
      if: always()
      uses: actions/upload-artifact@v3
      with:
        name: load-test-reports
        path: staging-load-test-report.json

  approve-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    if: github.event_name == 'push'
    environment:
      name: production
      approval: manual
    
    steps:
    - run: echo "Approved for production deployment"

  deploy-production:
    needs: approve-production
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Configure kubectl for production
      run: |
        aws eks update-kubeconfig --region us-east-1 --name pulsegrid-prod
      env:
        AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
        AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    
    - name: Deploy to production
      run: |
        kubectl set image deployment/pulsegrid-api api=${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:api-${{ github.sha }} -n pulsegrid
        kubectl set image deployment/pulsegrid-worker worker=${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com/pulsegrid:worker-${{ github.sha }} -n pulsegrid
        kubectl rollout status deployment/pulsegrid-api -n pulsegrid --timeout=5m
        kubectl rollout status deployment/pulsegrid-worker -n pulsegrid --timeout=5m
    
    - name: Verify production health
      run: |
        for i in {1..30}; do
          if curl -f https://api.pulsegrid.com/health; then
            echo "Production API healthy"
            exit 0
          fi
          sleep 10
        done
        echo "Production API failed health check"
        exit 1
```

## Load Test Harness

### Configuration Schema

```json
{
  "test_name": "baseline-load-test",
  "duration_seconds": 3600,
  "api_endpoint": "https://api.pulsegrid.example.com",
  "jobs": {
    "total_jobs": 500,
    "burst_per_minute": 50,
    "burst_ramp_up_minutes": 5,
    "ramp_down_minutes": 2
  },
  "video_config": {
    "file_size_mb": 1024,
    "sample_video_s3": "s3://test-videos/sample-1gb.mp4",
    "use_generated": false
  },
  "renditions": [
    {"id": "720p", "resolution": "1280x720", "bitrate": "5M"},
    {"id": "480p", "resolution": "854x480", "bitrate": "2.5M"},
    {"id": "hls", "type": "hls_segments"}
  ],
  "poll_config": {
    "poll_interval_seconds": 5,
    "max_poll_attempts": 720,
    "timeout_seconds": 3600
  },
  "slos": {
    "p50_latency_seconds": 300,
    "p95_latency_seconds": 600,
    "p99_latency_seconds": 1800,
    "success_rate_percent": 99.5,
    "scale_up_time_seconds": 60,
    "scale_down_time_seconds": 600
  }
}
```

### Harness Output

**JSON Report** (`load-test-report.json`):
```json
{
  "test_name": "baseline-load-test",
  "start_time": "2024-01-15T10:00:00Z",
  "end_time": "2024-01-15T11:00:00Z",
  "duration_seconds": 3600,
  "summary": {
    "jobs_submitted": 500,
    "jobs_completed": 498,
    "jobs_failed": 2,
    "success_rate_percent": 99.6,
    "dlq_jobs": 0
  },
  "latency": {
    "p50_seconds": 245,
    "p95_seconds": 480,
    "p99_seconds": 1650,
    "min_seconds": 120,
    "max_seconds": 1850,
    "mean_seconds": 350
  },
  "rendition_breakdown": {
    "720p": {
      "success_count": 498,
      "failure_count": 0,
      "avg_latency_seconds": 180,
      "p99_latency_seconds": 350
    },
    "480p": {
      "success_count": 498,
      "failure_count": 0,
      "avg_latency_seconds": 120,
      "p99_latency_seconds": 250
    }
  },
  "scaling_events": [
    {
      "timestamp": "2024-01-15T10:02:15Z",
      "event_type": "scale_up",
      "from_replicas": 1,
      "to_replicas": 15,
      "reason": "queue_depth_exceeded_threshold"
    },
    {
      "timestamp": "2024-01-15T11:05:00Z",
      "event_type": "scale_down",
      "from_replicas": 15,
      "to_replicas": 1,
      "reason": "queue_empty"
    }
  ],
  "scale_up_latency_seconds": 42,
  "scale_down_latency_seconds": 305,
  "slo_validation": {
    "p50_latency_ok": true,
    "p95_latency_ok": true,
    "p99_latency_ok": true,
    "success_rate_ok": true,
    "scale_up_time_ok": true,
    "scale_down_time_ok": false,
    "violations": [
      "Scale-down took 305 seconds (SLO: 600 seconds) - PASS"
    ]
  }
}
```

**Markdown Summary** (`load-test-report.md`):
```markdown
# Load Test Report: baseline-load-test

**Test Duration**: 2024-01-15 10:00 - 11:00 UTC (3600 seconds)

## Summary

- **Jobs Submitted**: 500
- **Jobs Completed**: 498
- **Jobs Failed**: 2 (0.4%)
- **Success Rate**: 99.6% ✅

## Latency (Video transcoding time)

| Metric | Value | SLO | Status |
|--------|-------|-----|--------|
| P50 | 245s (4m 5s) | 300s (5m) | ✅ Pass |
| P95 | 480s (8m 0s) | 600s (10m) | ✅ Pass |
| P99 | 1650s (27m 30s) | 1800s (30m) | ✅ Pass |
| Min | 120s | - | - |
| Max | 1850s | - | - |

## Autoscaling Performance

| Metric | Value | SLO | Status |
|--------|-------|-----|--------|
| Scale-up Time | 42s | 60s | ✅ Pass |
| Scale-down Time | 305s | 600s | ✅ Pass |
| Max Replicas Reached | 15 | 100 | ✅ OK |

## Per-Rendition Breakdown

### 720p
- Success: 498/498
- Avg Latency: 180s
- P99 Latency: 350s

### 480p
- Success: 498/498
- Avg Latency: 120s
- P99 Latency: 250s

## Scaling Events

1. **10:02:15** - Scale up: 1 → 15 replicas (queue depth exceeded)
2. **11:05:00** - Scale down: 15 → 1 replicas (queue empty)

## Recommendations

✅ All SLOs met. System is ready for production deployment.
```



## API Request/Response Examples

### Upload Video

**Request**:
```http
POST /videos/upload HTTP/1.1
Host: api.pulsegrid.example.com
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary

------WebKitFormBoundary
Content-Disposition: form-data; name="video"; filename="marketing_video.mp4"
Content-Type: video/mp4

[Binary video data - 1.5 GB]
------WebKitFormBoundary
Content-Disposition: form-data; name="source_name"

marketing_video_2024
------WebKitFormBoundary
Content-Disposition: form-data; name="renditions"

[
  {"id": "720p", "resolution": "1280x720", "bitrate": "5M"},
  {"id": "480p", "resolution": "854x480", "bitrate": "2.5M"},
  {"id": "hls", "type": "hls_segments"}
]
------WebKitFormBoundary--
```

**Response** (HTTP 202 Accepted):
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status_uri": "/jobs/550e8400-e29b-41d4-a716-446655440000",
  "estimated_wait_time_seconds": 180,
  "submission_time": "2024-01-15T10:30:00Z"
}
```

### Query Job Status

**Request**:
```http
GET /jobs/550e8400-e29b-41d4-a716-446655440000 HTTP/1.1
Host: api.pulsegrid.example.com
```

**Response** (HTTP 200 OK):
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "submission_time": "2024-01-15T10:30:00Z",
  "completion_time": "2024-01-15T10:35:15Z",
  "source_file_name": "marketing_video_2024.mp4",
  "source_file_size_bytes": 1610612736,
  "retry_count": 0,
  "output_files": [
    {
      "rendition": "720p",
      "path": "s3://pulsegrid-output/550e8400-.../720p/720p.mp4",
      "size_bytes": 524288000,
      "duration_seconds": 300
    },
    {
      "rendition": "480p",
      "path": "s3://pulsegrid-output/550e8400-.../480p/480p.mp4",
      "size_bytes": 262144000,
      "duration_seconds": 300
    },
    {
      "rendition": "hls",
      "path": "s3://pulsegrid-output/550e8400-.../hls/playlist.m3u8",
      "segment_count": 51,
      "segment_duration_seconds": 6
    }
  ],
  "failure_reason": null
}
```

### Query Job History

**Request**:
```http
GET /jobs?submitted_after=2024-01-15T00:00:00Z&submitted_before=2024-01-15T12:00:00Z&status=completed&limit=100 HTTP/1.1
Host: api.pulsegrid.example.com
```

**Response** (HTTP 200 OK):
```json
{
  "jobs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "completed",
      "submission_time": "2024-01-15T10:30:00Z",
      "completion_time": "2024-01-15T10:35:15Z",
      "duration_seconds": 315
    },
    {
      "job_id": "660e8400-e29b-41d4-a716-446655440001",
      "status": "completed",
      "submission_time": "2024-01-15T11:00:00Z",
      "completion_time": "2024-01-15T11:06:30Z",
      "duration_seconds": 390
    }
  ],
  "total": 2,
  "limit": 100,
  "offset": 0
}
```

## Configuration Examples

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pulsegrid-defaults
  namespace: pulsegrid
data:
  max_upload_size_gb: "10"
  default_renditions: |
    [
      {
        "id": "720p",
        "resolution": "1280x720",
        "video_codec": "libx264",
        "video_bitrate": "5M",
        "audio_codec": "aac",
        "audio_bitrate": "128k"
      },
      {
        "id": "480p",
        "resolution": "854x480",
        "video_codec": "libx264",
        "video_bitrate": "2.5M",
        "audio_codec": "aac",
        "audio_bitrate": "96k"
      },
      {
        "id": "hls",
        "type": "hls_segments",
        "segment_duration_seconds": 6,
        "base_resolution": "720p"
      }
    ]
  kafka_retention_days: "7"
  db_connection_pool_size: "20"
  max_retries: "3"
  visibility_timeout_seconds: "1800"
  s3_lifecycle_glacier_days: "90"
  s3_lifecycle_delete_days: "365"
```

### Worker Pod Environment

```bash
# In worker pod container
export KAFKA_BROKERS=kafka-0.kafka.pulsegrid.svc.cluster.local:9092
export CONSUMER_GROUP=pulsegrid-workers
export JOB_TOPIC=transcoding-jobs
export DLQ_TOPIC=transcoding-dlq
export MAX_RETRIES=3
export VISIBILITY_TIMEOUT=1800
export S3_BUCKET_OUTPUT=pulsegrid-output
export S3_BUCKET_SOURCE=pulsegrid-source
export AWS_REGION=us-east-1
export FFMPEG_LOG_LEVEL=info
export TEMP_DIR=/tmp
export METRICS_PORT=8081
```

## Design Decisions and Rationale

### 1. Kafka for Job Queue (vs. SQS, RabbitMQ)
- **Rationale**: Kafka provides reliable at-least-once delivery, consumer groups for parallel processing, and Dead Letter Queue support. High throughput (100+ jobs/min easily achievable). Topic retention allows replay if needed.
- **Alternative Considered**: AWS SQS (simpler, managed), but limited DLQ support and visibility timeout model less intuitive than Kafka.

### 2. Postgres + TimescaleDB (vs. DynamoDB, MongoDB)
- **Rationale**: TimescaleDB provides optimized time-series queries for analytics. Strong consistency for transactional job metadata. Mature backup/recovery. SQL allows complex queries (percentiles, time ranges).
- **Alternative Considered**: DynamoDB (serverless), but limited query flexibility and complex time-series analytics.

### 3. S3 for Outputs (vs. Local NAS, GCS)
- **Rationale**: S3 is global, durable (99.999999999%), cost-effective at scale. Lifecycle policies automate archive/deletion. Built-in versioning and tagging.
- **Alternative Considered**: GCS (similar), but AWS-native integration simpler.

### 4. KEDA for Autoscaling (vs. Custom HPA, Step Functions)
- **Rationale**: KEDA provides Kafka lag-based scaling out of the box. More sophisticated than Kubernetes HPA. No need for custom controller.
- **Alternative Considered**: Custom HPA with external metrics (more complex), AWS Step Functions (serverless but vendor-locked).

### 5. UUID v4 for Job IDs (vs. Sequential, Nanoid)
- **Rationale**: Globally unique without coordination, collision probability negligible, sortable by timestamp if using v6. No central ID server needed.
- **Alternative Considered**: Sequential IDs (simpler, smaller), Nanoid (shorter), but UUID v4 is industry standard.

### 6. Graceful Shutdown with SIGTERM (vs. Abrupt Termination)
- **Rationale**: Kubernetes sends SIGTERM before pod termination. Worker pod should complete in-flight job or report failure cleanly. Prevents orphaned jobs and ensures Kafka offset is committed.
- **Alternative Considered**: Ignore SIGTERM (simpler, but queue can re-process job unnecessarily).

### 7. Dual Testing: PBT + Integration (vs. Only Unit Tests)
- **Rationale**: Property-based tests find edge cases in logic. Integration tests verify external service contracts. Together, they provide comprehensive coverage without brittleness.
- **Alternative Considered**: Only unit tests (faster but miss integration bugs), only integration tests (slower, less coverage of logic variants).

## Deployment Topology

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AWS Region (us-east-1)                          │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                   EKS Cluster                                  │ │
│  │                                                                │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │ │
│  │  │ API Pod  │  │ API Pod  │  │ API Pod  │ (replicas: 3)      │ │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘                    │ │
│  │       │             │             │                          │ │
│  │       └─────────────┼─────────────┘                          │ │
│  │                     │ Service (LoadBalancer)                 │ │
│  │                     │ pulsegrid-api:80                       │ │
│  │                     │                                        │ │
│  │  ┌──────────────────┼──────────────────┐                     │ │
│  │  │                  │                  │                     │ │
│  │  ▼                  ▼                  ▼                     │ │
│  │ ┌────────┐   ┌────────┐   ┌────────┐ ...                   │ │
│  │ │ Worker │   │ Worker │   │ Worker │ (1-100 replicas)      │ │
│  │ │  Pod   │   │  Pod   │   │  Pod   │ Scaled by KEDA        │ │
│  │ └────────┘   └────────┘   └────────┘                       │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐   │ │
│  │  │        Kafka Cluster (StatefulSet)                  │   │ │
│  │  │ kafka-0, kafka-1, kafka-2 (replicas: 3)             │   │ │
│  │  │ Topics: transcoding-jobs, transcoding-dlq           │   │ │
│  │  └──────────────────────────────────────────────────────┘   │ │
│  │                                                              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │         AWS Managed Services (VPC Endpoints)                   │ │
│  │                                                                │ │
│  │  RDS: pulsegrid-db (Postgres Multi-AZ)                       │ │
│  │  S3:  pulsegrid-source, pulsegrid-output (buckets)          │ │
│  │  Monitoring: CloudWatch Logs, CloudWatch Metrics            │ │
│  │                                                                │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

External:
┌─────────────────────────────────────────────────────────────────┐
│            Monitoring & Observability (Optional SaaS)            │
│                                                                   │
│  Prometheus: Scrapes /metrics from pods (30s interval)           │
│  Grafana:    Dashboards, alerting, SLO tracking                 │
│  CloudWatch: Logs aggregation, cost tracking                    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Summary

This design provides a production-grade distributed video transcoding platform with:

1. **Horizontal Scalability**: Worker pods scale 1-100 based on Kafka queue depth
2. **High Availability**: Multi-AZ Postgres, replicated Kafka, stateless API/workers
3. **Fault Tolerance**: Dead Letter Queue for failed jobs, graceful shutdown, automatic recovery
4. **Observable**: Comprehensive Prometheus metrics, Grafana dashboards, structured logging
5. **Tested**: Property-based tests for logic, integration tests for services, load testing for performance
6. **Reproducible**: Terraform IaC, Kubernetes manifests, CI/CD automation
7. **Cost-Optimized**: S3 lifecycle policies, spot instance support, right-sizing recommendations

The platform targets 99.5% job success rate, p99 latency under 30 minutes for 1GB videos, and scales from idle to 100 pods within 60 seconds.
