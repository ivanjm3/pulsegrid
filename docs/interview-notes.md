# Pulsegrid Interview Notes

## UUID v4 Job ID Generation

### Interview Questions

- Why UUID v4 over sequential IDs?
- Why `crypto/rand` instead of `math/rand`?
- What guarantees does UUID v4 give on uniqueness?
- How does RFC 4122 version/variant bits work?
- What's the collision probability for UUID v4?

### Follow-up Questions

- How would you handle ID collisions at scale?
- Could you use ULIDs instead? Tradeoffs?
- How does UUID v4 affect database indexing performance?
- What happens if the system entropy pool is exhausted?

### Resume Talking Points

- Chose crypto/rand for cryptographic randomness (not predictable)
- Property-tested with 200+ iterations for uniqueness + RFC 4122 compliance
- No external dependencies — stdlib only

---

## Core Type Design (Job, Rendition, JobStatus, RetryConfig)

### Interview Questions

- Why use a string enum pattern for JobStatus in Go?
- How do you ensure type safety with string-based statuses?
- Why are pointer types used for optional timestamps?
- How does the Rendition struct handle both MP4 and HLS output types?

### Follow-up Questions

- How would you add a new status without breaking existing consumers?
- How would you version the Job struct for schema evolution?
- Why JSONB for renditions in Postgres vs normalized tables?

### Resume Talking Points

- Designed flexible Rendition struct supporting both single-file and HLS segment outputs
- Used pointer types for nullable time fields (idiomatic Go)
- RetryConfig with exponential backoff (1s base, 16s cap, max 3 retries)

---

## HTTP Server & Multipart Upload Parsing

### Interview Questions

- Why use `http.MaxBytesReader` instead of checking Content-Length header?
- How does multipart/form-data streaming work vs buffering entire file in memory?
- Why return 202 Accepted instead of 200 OK for upload?
- How do you validate JSON embedded in a form field?
- What's the difference between 400 and 413 status codes semantically?
- Why generate a request_id separate from job_id for error tracing?

### Follow-up Questions

- How would you handle concurrent uploads exhausting memory?
- What happens if MaxBytesReader triggers mid-stream — does client get partial response?
- How would you add rate limiting per client?
- How would you validate video file magic bytes vs trusting Content-Type?
- What are TOCTOU risks with file size validation?

### Resume Talking Points

- Implemented streaming upload validation with MaxBytesReader (no full buffering)
- Structured error responses with request_id for distributed tracing
- Modular handler design — parse/validate layer separated from S3/Kafka/DB integration
- Schema validation on renditions JSON (requires id or type per rendition)
- Stdlib-only HTTP server — no external framework dependencies
- Test strategy: override MaxUploadSize in tests to validate 413 path without streaming 10GB

---

## HTTP Request Validation Testing

### Interview Questions

- How do you test file size limits without actually streaming 10GB in a unit test?
- What is `http.MaxBytesReader` and why is it preferred over Content-Length checks?
- Why use a package-level variable for MaxUploadSize instead of a constant?
- How does `io.Pipe` enable streaming multipart tests without buffering?
- What's the difference between 400, 413, and 415 HTTP status codes?

### Follow-up Questions

- How would you test MaxBytesReader behavior at exact boundary (limit ± 1 byte)?
- What happens if a client sends chunked transfer encoding with no Content-Length?
- How does MaxBytesReader interact with connection reuse (HTTP keep-alive)?
- Why is the error string comparison fragile — what alternatives exist?

### Resume Talking Points

- Used io.Pipe + goroutine to simulate streaming upload exceeding size limit
- Made MaxUploadSize a testable variable (override in test, restore via defer)
- Validated full error response structure (error_code, request_id, timestamp)
- Zero-reader pattern for generating arbitrary-size test payloads without memory allocation

---

## Custom Error Types (TranscodingError, ResourceConstraintError)

### Interview Questions

- Why custom error types vs sentinel errors?
- How does Go's error interface enable polymorphic error handling?
- Why separate transient (TranscodingError) from fatal (ResourceConstraintError)?
- How does error classification drive retry vs DLQ decisions?

### Follow-up Questions

- How would you add error wrapping/unwrapping?
- How do you test error type assertions in Go?
- What's the pattern for adding context to errors across call boundaries?

### Resume Talking Points

- Error taxonomy drives system behavior: retry vs DLQ vs pod exit
- Structured errors carry context (job ID, resource type, stderr) for observability
- Clean separation enables type-switch based error handling in worker loop


---

## S3 Multipart Upload (Streaming, No Disk Buffering)

### Interview Questions

- Why use multipart upload instead of single PutObject for large files?
- How does S3 multipart upload work under the hood (InitiateMultipartUpload → UploadPart → CompleteMultipartUpload)?
- Why stream from io.Reader directly vs buffering to local disk first?
- What happens if one part fails mid-upload?
- How does the AWS SDK v2 `s3/manager.Uploader` handle part sizing and concurrency?
- Why tag S3 objects vs storing metadata in a separate DB?
- How does URL-encoding in S3 object tags work?

### Follow-up Questions

- How would you resume a failed multipart upload?
- What's the maximum part count / minimum part size for S3 multipart?
- How would you handle network interruption during a 10GB upload?
- What are the cost implications of incomplete multipart uploads?
- How would you set up lifecycle rules to abort incomplete multipart uploads?
- How does S3 Transfer Acceleration compare to multipart for global clients?

### Resume Talking Points

- Implemented streaming multipart S3 upload (no local disk = handles 10GB files in low-memory pods)
- Used `s3/manager.Uploader` with 10MB parts, 5 concurrent uploads
- Object tagging for traceability (job_id, upload_time, source_name)
- Interface-based design (`S3Uploader`) for testability and local dev

---

## Exponential Backoff Retry Pattern

### Interview Questions

- Why exponential backoff vs fixed-interval retry?
- What is jitter and when would you add it to backoff?
- How do you prevent retry storms in distributed systems?
- Why cap the maximum delay (16s in this case)?
- How does context cancellation integrate with retry loops?
- What's the difference between retryable and non-retryable errors?

### Follow-up Questions

- How would you add jitter to prevent thundering herd?
- How does circuit breaker pattern differ from retry with backoff?
- When would you use exponential backoff vs linear backoff?
- How do you decide max attempts — what factors matter?
- How would you make the retry observable (metrics, logging)?
- How does AWS SDK's built-in retry differ from application-level retry?

### Resume Talking Points

- Generic `RetryWithBackoff` function: context-aware, capped exponential delays
- Delays: 1s → 2s → 4s → 8s → 16s (base * 2^attempt, capped)
- Context cancellation respected between attempts (no wasted work)
- Reusable utility — same function for S3, Kafka, DB retries


---

## S3 Upload Unit Testing Strategy

### Interview Questions

- How do you test S3 uploads without hitting real AWS?
- Why mock at interface level vs HTTP level?
- How do you verify retry behavior with exponential backoff in tests?
- What's the difference between testing transient vs permanent S3 errors?
- How do you validate S3 object tagging in unit tests?
- Why use `smithy-go/transport/http.ResponseError` for simulating AWS errors?

### Follow-up Questions

- How would you test multipart upload specifically (part failures, incomplete uploads)?
- When would you use `httptest` server vs interface mocks for AWS SDK testing?
- How do you avoid flaky timing-dependent retry tests?
- How would you test that AccessDenied maps to HTTP 500 in the handler layer?
- What's the tradeoff between fast test delays (10ms) vs production delays (1s)?

### Resume Talking Points

- Interface-based mock (`S3Uploader`) enables isolated retry/error testing without AWS
- Used `smithy-go` error types for realistic AWS error simulation (403 AccessDenied)
- Validated tag URL-encoding handles special characters (spaces, parens)
- Retry tests verify both transient-then-success and permanent-failure-exhaustion paths
- Short backoff delays (10ms) in tests keep suite fast while preserving behavioral coverage


---

## Kafka Producer Integration (Job Queue)

### Interview Questions

- Why use Kafka over SQS/RabbitMQ for job distribution?
- How does partitioning by job_id hash affect processing order?
- Why `RequireAll` acks vs `RequireOne`? Throughput vs durability tradeoff?
- How does at-least-once delivery differ from exactly-once?
- Why separate DLQ topic vs re-enqueue with retry count?
- How does visibility timeout work without native Kafka support?
- Why use `segmentio/kafka-go` over Sarama or confluent-kafka-go?
- How does the Hash balancer distribute messages across 32 partitions?

### Follow-up Questions

- How would you implement exactly-once semantics with Kafka transactions?
- What happens if Kafka broker is down during enqueue? How does client retry help?
- How would you handle message ordering across retries?
- What's the impact of 32 partitions on consumer parallelism?
- How would you monitor Kafka producer latency and error rate?
- How does consumer group rebalancing affect in-flight jobs?
- What happens if DLQ write also fails?
- How would you implement backpressure from Kafka to the API?

### Resume Talking Points

- Interface-based design (`KafkaProducer`) — same pattern as S3, mockable and nil-safe for local dev
- Exponential backoff retry (1s base, 16s cap, 5 attempts) — reuses generic `RetryWithBackoff` utility
- Partition by job_id hash ensures same job always lands on same partition (ordering guarantee per job)
- RequireAll acks for strongest durability (message replicated to all ISR before ack)
- Separate DLQ writer with own retry — failed jobs don't block main topic writes
- JSON serialization with explicit schema struct (`KafkaMessage`) — not raw Job struct


---

## Kafka Message Schema Property Testing

### Interview Questions

- Why test serialization round-trip instead of real Kafka broker in unit tests?
- How does `testing/quick` generate random inputs for property tests?
- Why verify JSON field names via raw map instead of just struct unmarshal?
- What's the difference between schema validation and data validation?
- Why check both raw map types AND struct round-trip?
- How do you ensure 0-5 renditions covers edge cases (empty array)?

### Follow-up Questions

- How would you test schema evolution (adding fields) without breaking consumers?
- What happens if a required field is added to KafkaMessage but not populated?
- How would you enforce schema at Kafka level (Schema Registry, Avro)?
- What are tradeoffs of JSON vs Protobuf vs Avro for Kafka messages?
- How would you detect schema drift between producer and consumer?

### Resume Talking Points

- Property-tested Kafka message schema with 150+ random inputs (0-5 renditions, varied codecs)
- Verified JSON round-trip preserves all required fields with correct types
- No broker dependency — tests serialization contract only (fast, deterministic)
- Uses stdlib `testing/quick` — no external property testing framework needed


---

## Postgres Integration (pgx, Connection Pooling, Job Tracking)

### Interview Questions

- Why pgx over database/sql + lib/pq?
- How does pgxpool manage connection lifecycle (idle timeout, health checks)?
- Why retry initial connection with exponential backoff?
- Why UUID as primary key vs BIGSERIAL?
- How does JSONB for renditions compare to normalized tables?
- Why separate jobs table from job_status_events?
- What is a TimescaleDB hypertable and when is it appropriate?
- How does CHECK constraint on status column enforce data integrity?
- Why use parameterized queries ($1, $2) instead of string interpolation?

### Follow-up Questions

- How would you handle connection pool exhaustion under load?
- What happens if Postgres is down at startup — should the API fail or start degraded?
- How would you implement optimistic concurrency on the jobs table (updated_at)?
- How would you partition the jobs table if it grows to billions of rows?
- What's the tradeoff of synchronous DB write in request path vs async write?
- How would you implement read replicas for status queries?
- How does pgxpool differ from PgBouncer — when use each?
- How would you handle schema migrations in production (zero-downtime)?
- What indexes would you add for a "get jobs by status" query?

### Resume Talking Points

- Used pgx/v5 native driver (not database/sql) — zero overhead, full Postgres feature set
- Connection pool with retry-on-connect (same exponential backoff pattern as S3/Kafka)
- Interface-based design (`DBClient`) — nil-safe for local dev, mockable for tests
- TimescaleDB hypertable for job_status_events — optimized for time-series append/query
- JSONB renditions column avoids complex joins for read-heavy status queries
- Migrations in plain SQL (no ORM migration tool) — explicit, reviewable, version-controlled


---

## Database Round-Trip Property Testing (GetJobByID)

### Interview Questions

- Why test round-trip with mock instead of real DB in unit tests?
- How does `testing/quick` generate random structs for property tests?
- Why verify all fields individually vs `reflect.DeepEqual` on entire struct?
- What does `GetJobByID` need to handle that `RecordJobMetadata` doesn't (deserialization)?
- Why use JSONB for renditions — what round-trip issues can occur with JSON marshal/unmarshal?
- How do you ensure time.Time equality across serialization boundaries?

### Follow-up Questions

- What happens if a field is added to Job struct but not to GetJobByID scan list?
- How would you test round-trip with actual Postgres (integration test)?
- How does `reflect.DeepEqual` handle nil slice vs empty slice for Renditions?
- What edge cases exist for JSONB round-trip (Unicode, large arrays, nested objects)?
- How would you property-test concurrent inserts + reads for race conditions?

### Resume Talking Points

- Property-tested DB contract with 100+ random jobs (varied statuses, 0-5 renditions, random sizes/timestamps)
- Interface-based mock validates insert/query contract without real DB dependency
- Added `GetJobByID` to `DBClient` interface — enables status query endpoint (Requirement 5.5)
- Round-trip verifies: job_id, status, submission_time, renditions, source_file_name, source_file_size_bytes


---

## Atomic DB-Kafka Write Ordering (Orphan Prevention)

### Interview Questions

- Why write DB before Kafka instead of Kafka before DB?
- What is an "orphan" in this context and why is it dangerous?
- Why use status='submitting' as intermediate state?
- What happens if Kafka publish fails after DB insert?
- What happens if DB commit fails after Kafka publish?
- Why not use a distributed transaction (2PC) across DB and Kafka?
- How does this pattern compare to the Outbox Pattern?
- Why is the Kafka publish done outside the DB transaction?
- What guarantees does this give vs full exactly-once semantics?

### Follow-up Questions

- How would you detect and recover orphans (job in Kafka but not in DB)?
- Could you use Kafka transactions + idempotent producer instead?
- How would you implement the Outbox Pattern as an alternative?
- What monitoring/alerting would you set up for the ALERT log?
- How does this interact with Kafka's at-least-once delivery guarantee?
- What happens if the API pod crashes between Kafka publish and DB commit?
- How would you handle this in an eventually-consistent system?
- What are the tradeoffs of returning 202 when DB commit fails?

### Resume Talking Points

- Implemented atomic write ordering: DB(submitting) → Kafka → DB(submitted) → commit
- Prevents orphans: if Kafka fails, DB rollback makes job invisible to clients
- Handles edge case: if commit fails after Kafka publish, logs ALERT for operator investigation
- Chose pragmatic approach over distributed transactions (simpler, no 2PC coordinator needed)
- TxHandle interface abstracts transaction lifecycle for testability


---

## GET /jobs Range Query Endpoint (Paginated Filtering)

### Interview Questions

- Why use parameterized queries ($1, $2) instead of string interpolation for SQL?
- How does dynamic WHERE clause building handle SQL injection risks?
- Why return total count with paginated results — what's the cost?
- Why ORDER BY submission_time DESC — what index supports this?
- How do you validate ISO 8601 timestamps in Go?
- Why limit the max page size to 1000?
- Why separate COUNT(*) query from data query instead of using window functions?
- How does PostgreSQL handle IN clause with parameterized arrays?

### Follow-up Questions

- How would you optimize the COUNT(*) for large tables (millions of rows)?
- When would you use cursor-based pagination instead of offset/limit?
- How would you add full-text search to job queries?
- What happens if a client requests offset=999999 on a 1000-row table?
- How would you cache query results for frequently accessed ranges?
- How does the idx_jobs_submission_time index interact with status filtering?
- What are the tradeoffs of composite indexes (status, submission_time) vs separate?

### Resume Talking Points

- Dynamic SQL query builder with parameterized placeholders (no string concat of user input)
- Dual-query pagination: COUNT(*) for total + LIMIT/OFFSET for page
- ISO 8601 timestamp validation with range consistency check (after < before)
- Status whitelist validation prevents unexpected enum values
- Interface-based DBClient allows nil-safe local dev and mock testing


---

## Prometheus Metrics Instrumentation (API Server)

### Interview Questions

- Why emit metrics only on successful requests (not errors)?
- How does `prometheus.Counter` differ from `prometheus.Gauge`?
- Why use a histogram for upload duration instead of a summary?
- How do histogram buckets affect query performance in Prometheus?
- What's the difference between Prometheus push vs pull model?
- Why use a custom `prometheus.Registry` in tests?
- How does `time.Since()` precision affect sub-millisecond observations?
- Why place timing at handler entry vs after validation?

### Follow-up Questions

- How would you add labels (rendition count, file size bucket) without cardinality explosion?
- How does histogram quantile estimation work (Prometheus vs pre-computed summaries)?
- How would you alert on upload duration p99 exceeding 2 seconds?
- What happens if Prometheus scrape misses a counter increment?
- How would you add request-scoped metrics (per job_id) without unbounded cardinality?
- How does the /metrics endpoint interact with Kubernetes service discovery?
- When would you use a Summary instead of Histogram?

### Resume Talking Points

- Instrumented upload handler: counter (jobs_submitted_total) + histogram (upload_duration_seconds)
- Metrics emitted only on success path (after 202) — no noise from validation errors
- Custom histogram buckets (0.1s–300s) tuned for upload latency distribution
- Injectable `*Metrics` struct with custom registry — full test isolation, no global state leakage
- Tested: counter increments, histogram observation count, bucket boundaries, no-emit on failure


---

## Kafka Queue Depth Gauge (Background Polling)

### Interview Questions

- Why poll Kafka admin API for queue depth instead of using consumer lag metrics directly?
- How does partition end_offset minus committed_offset give queue depth?
- Why run polling in a background goroutine vs computing on each /metrics scrape?
- How do you handle broker unavailability in the polling loop?
- Why use `kafka.DialLeader` per partition vs a single admin connection?
- What's the difference between consumer lag and queue depth?
- Why default to 30-second poll interval — what tradeoffs exist?
- How does `OffsetFetch` request work in Kafka's group coordinator protocol?

### Follow-up Questions

- How would you handle partition rebalancing mid-poll?
- What happens if consumer group has never committed offsets (brand new group)?
- How would you optimize to avoid N connections for N partitions?
- How does this interact with KEDA's own queue depth polling?
- What happens if committed_offset > end_offset (log truncation, retention)?
- How would you add per-partition lag breakdown as separate metrics?
- What's the memory/connection overhead of polling 32 partitions every 30s?
- How would you test this without a running Kafka cluster?

### Resume Talking Points

- Implemented queue depth as Prometheus gauge — polls Kafka admin API every 30s
- Calculates sum(end_offset - committed_offset) across all partitions
- Graceful degradation: logs warning on poll failure, keeps last known value
- Uses kafka-go's `OffsetFetch` API to get consumer group committed offsets
- Same pattern as KEDA uses for autoscaling decisions — single source of truth for queue pressure


---

## Health Check Endpoint (Liveness/Readiness Probes)

### Interview Questions

- Why separate liveness from readiness probes in Kubernetes?
- Why check all dependencies (Postgres, Kafka, S3) instead of just returning 200?
- How does a 5-second timeout on health checks prevent cascading failures?
- Why return 503 instead of 500 when a dependency is down?
- How does `HeadBucket` differ from `ListBuckets` for S3 health checks?
- Why use `pool.Ping()` instead of running a SELECT 1 query?
- How does the health endpoint interact with Kubernetes pod lifecycle?
- Why show individual component status vs just aggregate pass/fail?

### Follow-up Questions

- How would you implement a degraded state (e.g., S3 down but Kafka up — can still accept jobs)?
- How would you prevent health check storms when Kafka broker is slow?
- What happens if health check itself is slow — does Kubernetes restart the pod?
- How would you implement circuit breaking for unhealthy dependencies?
- How does `failureThreshold` interact with probe periodSeconds?
- Why not cache health check results to avoid hammering dependencies?
- How would you distinguish between liveness (should I restart?) and readiness (should I route traffic?)?
- What's the risk of a false-positive health check (says healthy but can't actually serve requests)?

### Resume Talking Points

- Implemented per-component health check with structured response (status + error details)
- 5-second context timeout prevents slow dependencies from blocking probe response
- Interface-based design (`Ping()` on each client) — same pattern as mocking for tests
- Graceful handling of "not_configured" state for local dev mode (returns healthy)
- Kafka ping: TCP dial to broker (lightweight, no message publish)
- S3 ping: HeadBucket (verifies both connectivity and IAM permissions)
- Postgres ping: pgxpool.Ping (verifies pool has working connection, handles reconnect)


---

## API Functional Testing & Integration Test Patterns

### Interview Questions

- Why test the full upload flow (parse → S3 → Kafka → DB → 202) as a single test?
- How do you mock three dependencies simultaneously without test complexity explosion?
- Why use in-memory mock DB (map) vs a test database?
- How does interface-based design enable dependency injection for tests?
- What's the difference between unit tests and integration tests in this context?
- Why test the round-trip (upload → query same job) as a separate integration test?
- How do mock TxHandle patterns validate atomic write semantics?
- Why verify mock call counts (e.g., S3 uploadCalled, Kafka enqueuedJobs length)?

### Follow-up Questions

- How would you test against real Kafka/Postgres in CI without slowing tests?
- When would you use testcontainers vs interface mocks?
- How do you prevent test pollution when swapping global variables (s3Uploader, dbClient)?
- What are risks of defer-based restore patterns for global state in parallel tests?
- How would you test timeout/context cancellation across the full flow?
- How do you validate the integration test covers the same code path as production?
- What's the tradeoff between high-fidelity integration tests vs fast unit tests?
- How would you add contract tests between API and Worker (shared Kafka schema)?

### Resume Talking Points

- Integration tests verify full pipeline with all three mocks wired together (S3 + Kafka + DB)
- In-memory mock DB with TxHandle simulates atomic commit/rollback semantics
- Round-trip test (upload → query) validates data flows correctly between endpoints
- Mock verification: checked S3 received correct jobID, Kafka received correct message, DB has correct status
- Global variable swap with defer restore — fast isolation without DI framework
- Same mock interfaces used for both unit tests and integration tests (reusability)
- Covered: happy path, S3 failure, Kafka failure, DB commit failure, 404 not found


---

## Worker Pod: Kafka Consumer Loop & Graceful Shutdown

### Interview Questions

- Why use `kafka.Reader` (consumer group mode) instead of raw partition consumers?
- How does Kafka consumer group protocol differ from SQS visibility timeout?
- Why set `SessionTimeout` to 30 minutes for a transcoding worker?
- What is the relationship between `HeartbeatInterval` and `SessionTimeout`?
- How does `FetchMessage` + `CommitMessages` give at-least-once semantics?
- Why use an atomic bool for shutdown coordination instead of closing a channel?
- How does SIGTERM handling interact with Kubernetes pod termination grace period?
- Why commit offset even for malformed/failed messages?
- What happens if a worker crashes between `FetchMessage` and `CommitMessages`?
- Why expose a `/metrics` endpoint on a separate port (8081) from any future health endpoint?

### Follow-up Questions

- How would you implement exactly-once processing with Kafka (idempotent consumer)?
- What happens during consumer group rebalancing — does an in-flight job get duplicated?
- How would you handle a slow consumer that risks session timeout during a large transcode?
- Why not use `ReadMessage` (auto-commit) instead of `FetchMessage` + `CommitMessages`?
- How does `StartOffset: FirstOffset` differ from `LastOffset` for new consumer groups?
- What happens if two consumers in the same group commit the same offset?
- How would you add graceful draining (finish current job) vs hard kill (drop immediately)?
- What's the tradeoff of `MaxWait: 5s` vs shorter/longer poll timeouts?
- How would you monitor consumer lag per partition?
- How does Kubernetes `terminationGracePeriodSeconds` interact with 30-min session timeout?

### Resume Talking Points

- Implemented Kafka consumer with `segmentio/kafka-go` Reader in consumer group mode
- 30-minute session timeout prevents rebalance during long transcodes (matching visibility timeout concept)
- Graceful shutdown via SIGTERM: atomic bool stops new message fetch, in-flight job completes, then consumer closes
- At-least-once semantics: offset committed only after successful processing
- Separate metrics endpoint on :8081 — same pattern as API server for Prometheus scraping
- Environment-driven config (KAFKA_BROKERS, JOB_TOPIC, CONSUMER_GROUP, POD_ID) for K8s injection


---

## S3 Source Download (Worker Pod)

### Interview Questions

- Why stream S3 GetObject to disk instead of buffering in memory?
- How does exponential backoff interact with S3 transient errors (500, 503)?
- Why treat 404 (NoSuchKey) as a permanent failure instead of retrying?
- How does the worker distinguish transient vs permanent S3 errors?
- Why parse the s3:// URI in the download function instead of storing bucket/key separately?
- How does `os.TempDir()` behavior differ across OS/container environments?
- Why create the temp directory with `MkdirAll` instead of assuming it exists?
- What happens if two workers download the same job concurrently?

### Follow-up Questions

- How would you handle partial downloads (interrupted stream mid-copy)?
- What disk space checks would you add before starting a large download?
- How would you implement download progress reporting for observability?
- What happens if /tmp fills up during download — how does the worker recover?
- How would you verify file integrity after download (checksum/ETag)?
- How does this interact with Kubernetes ephemeral storage limits?
- What's the risk of io.Copy without a size limit on untrusted S3 content?
- How would you implement bandwidth throttling to avoid starving other pods?

### Resume Talking Points

- Implemented streaming S3 download with retry + permanent-error short-circuit
- Used `errors.As` pattern to distinguish NoSuchKey (404) from transient network errors
- Interface-based design (`S3Downloader`) — same testability pattern as upload path
- Logs download size + elapsed time for operational visibility
- Temp file staging at `/tmp/{jobID}/original.mp4` — cleaned up after processing


---

## Worker Pod: ffmpeg Invocation for Single Rendition

### Interview Questions

- Why use `exec.CommandContext` with timeout instead of raw `exec.Command`?
- How does context cancellation propagate to kill the ffmpeg child process?
- Why capture combined stdout/stderr instead of separate streams?
- How do you parse duration from ffmpeg output — why regex over structured output?
- Why truncate stderr to 500 chars in TranscodingError?
- Why place output file in same directory as source (temp dir)?
- How does the 30-minute timeout protect against hung ffmpeg processes?
- Why skip HLS-type renditions in the single-rendition path?

### Follow-up Questions

- How would you handle ffmpeg progress reporting (percent complete)?
- What happens if context is cancelled mid-transcode — is the partial output file cleaned up?
- How would you support GPU-accelerated encoding (NVENC) in the ffmpeg args?
- What are risks of CombinedOutput for very large stderr (memory)?
- How would you validate ffmpeg is installed at pod startup vs failing at first job?
- How would you parallelize multiple renditions within a single job?
- What happens if os.Stat fails due to race condition (file deleted between ffmpeg exit and stat)?

### Resume Talking Points

- Designed `TranscodeSingleRendition` with context-based timeout (30 min default) — prevents zombie ffmpeg processes
- Dynamic ffmpeg arg construction from Rendition struct — supports arbitrary codec/bitrate/resolution combos
- Error taxonomy: TranscodingError carries truncated stderr for debugging without memory bloat
- Duration parsing via regex on ffmpeg output — no external probe dependency
- Integrated into worker loop: iterates non-HLS renditions, collects TranscodeResult metadata


---

## Worker Pod: HLS Segment Generation (TranscodeHLS)

### Interview Questions

- Why produce HLS segments separately from single-file renditions?
- How does `-hls_time 6` control segment duration — is it exact or approximate?
- Why use `-hls_list_size 0` in the ffmpeg command?
- Why create a dedicated "hls" subdirectory instead of mixing with MP4 outputs?
- How does `-hls_segment_filename` pattern control segment naming?
- Why default to 6-second segments if SegmentDuration is 0?
- How do you verify HLS output correctness after ffmpeg exits?
- Why glob for `*.ts` instead of counting from playlist?
- What's the difference between MPEG-TS segments and fMP4 segments in HLS?
- Why return TranscodingError when no segments found after successful ffmpeg exit?

### Follow-up Questions

- How would you handle adaptive bitrate (multiple renditions in one master playlist)?
- What's the tradeoff between segment duration and seek latency?
- How would you validate playlist.m3u8 syntax (EXTINF tags, sequence numbers)?
- How would you support CMAF/fMP4 segments instead of MPEG-TS?
- What happens if disk fills mid-segment — how does ffmpeg report it?
- How would you generate byte-range HLS (single file, multiple ranges)?
- How does segment count affect CDN cache efficiency?
- How would you add encryption (AES-128 or SAMPLE-AES) to HLS segments?
- What is EXT-X-TARGETDURATION and how does it relate to -hls_time?

### Resume Talking Points

- Implemented `TranscodeHLS` with same patterns as single-rendition path (context timeout, stderr truncation, structured errors)
- HLS output: playlist.m3u8 + segment-XXXXX.ts naming convention
- Post-ffmpeg verification: stat playlist + glob segments — returns TranscodingError if missing
- Returns `HLSResult` struct with rendition_id, playlist path, segment count, segment file list
- Default 6-second segments (industry standard for live/VOD balance)
- Supports resolution scaling via `-s` flag from `Rendition.BaseResolution`

---

## Worker Pod: Manifest Generation

### Interview Questions

- Why generate a manifest.json instead of relying on S3 listing for output discovery?
- How does `json.MarshalIndent` + `json.Unmarshal` validate JSON correctness?
- Why read HOSTNAME env for worker_pod_id instead of passing it as function argument?
- How do you get ffmpeg version at runtime — what happens if ffmpeg not installed?
- Why include generation_time in ISO 8601 (RFC3339) specifically?
- What's the tradeoff between including file checksums in manifest vs just sizes?
- Why write manifest to temp dir before S3 upload instead of streaming directly?

### Follow-up Questions

- How would you add content checksums (SHA-256) to each output file entry?
- How would you version the manifest schema for backward compatibility?
- What happens if os.Stat fails on an HLS segment — partial manifest or error?
- How would you validate manifest against a JSON Schema before upload?
- Could you use protobuf instead of JSON for the manifest? Tradeoffs?
- How would you handle manifest generation if ffmpeg version command hangs?
- What if HOSTNAME is empty in a non-Kubernetes environment — fallback strategy?

### Resume Talking Points

- Manifest includes full pipeline traceability: job_id, worker_pod_id, ffmpeg_version, generation_time
- JSON validated via marshal + unmarshal round-trip before writing to disk
- Aggregates both single-rendition (TranscodeResult) and HLS (HLSResult) outputs into unified schema
- Fallback patterns: "unknown" for missing HOSTNAME or ffmpeg — graceful degradation over crash
- Written to `{tempDir}/manifest.json` — same lifecycle as transcoded outputs (uploaded then cleaned)


---

## Worker Pod: S3 Output Upload (UploadOutputsToS3)

### Interview Questions

- Why upload outputs sequentially per file instead of in parallel?
- How does `isPermanentS3Error` distinguish 403 from 503?
- Why use the same `RetryWithBackoff` for output uploads as source uploads?
- Why re-open the file on each retry attempt instead of seeking to offset 0?
- How does the S3 output key structure (`{jobID}/{rendition}/{filename}`) enable efficient listing?
- Why tag output objects with `completion_time` instead of relying on S3 object metadata?
- What happens if one rendition upload succeeds but the next fails — partial state?

### Follow-up Questions

- How would you implement parallel upload across renditions?
- How would you roll back successfully uploaded files if a later upload fails?
- What happens if the manifest upload fails after all renditions succeed?
- How would you verify uploaded file integrity (checksum/ETag comparison)?
- How would you handle S3 throttling (SlowDown) across many concurrent workers?
- What's the cost of S3 PUT requests at scale (1000+ files per job for HLS)?
- How would you implement resumable uploads for very large renditions?

### Resume Talking Points

- Interface-based design (`S3OutputUploader`) — same testability pattern as download/source upload
- Permanent error detection via smithy-go `ResponseError` HTTP status inspection (403 → no retry)
- Reuses `RetryWithBackoff` (5 attempts, exponential capped at 16s) for transient errors
- Tags all output objects with job_id, completion_time, rendition for lifecycle/audit
- Handles MP4, HLS (playlist + segments), and manifest uploads in single function


---

## Worker Pod: Job Completion, Retry/DLQ, and Error Classification

### Interview Questions

- Why classify errors into three categories (retryable, permanent, constraint) instead of just retry/no-retry?
- How does `errors.As` enable error classification through wrapped error chains?
- Why use string pattern matching as fallback when typed errors aren't available?
- Why exit the pod immediately on resource constraint instead of just failing the job?
- Why commit Kafka offset after re-enqueue (vs letting session timeout handle redelivery)?
- How does the DLQ message include pod_id — why is pod identity useful for debugging?
- Why set max retries at 3 — what tradeoff does this represent?
- How does re-enqueue to same topic differ from Kafka's built-in retry topic pattern?
- Why emit failure metrics with error_type label — how does this enable alerting?

### Follow-up Questions

- How would you add jitter between retries to prevent thundering herd on transient failures?
- What happens if the DLQ publish itself fails — do you lose the job?
- How would you implement dead-letter replay (move jobs back from DLQ to main topic)?
- How does `os.Exit(1)` interact with Kubernetes restart policy and backoff?
- What monitoring would you build around the constraint exit path?
- How would you handle partial success (3 of 4 renditions done, 1 fails)?
- Why not use a separate retry topic with increasing delays (Kafka retry pattern)?
- How would you prevent a permanently-failing job from counting against pod health?
- What happens if error classification is wrong (transient classified as permanent)?

### Resume Talking Points

- Implemented three-tier error classification: retryable → re-enqueue, permanent → DLQ, constraint → pod exit
- Error classification uses type assertion (`errors.As`) + string pattern fallback for untyped errors
- Re-enqueue increments retry_count in message — consumer sees updated count on next attempt
- DLQ messages include full context: job_id, failure_reason, timestamp, pod_id, retry_count
- Resource constraint triggers `os.Exit(1)` — Kubernetes restarts pod, Kafka rebalance redelivers job
- Metrics labeled by error_type enable per-category alerting (separate dashboards for transient vs permanent)
- Offset always committed after handling (re-enqueue or DLQ) — prevents double-processing


---

## Worker Pod: Temporary File Cleanup

### Interview Questions

- Why use `defer` for cleanup instead of explicit cleanup at each return point?
- Why `os.RemoveAll` instead of removing individual files?
- How does `filepath.Dir(localPath)` reliably identify the temp directory?
- Why log cleanup results instead of silently discarding errors?
- Why handle permission errors gracefully (log warning) instead of returning an error?
- What happens if cleanup runs after a partial download failure?
- Why clean up on both success AND failure paths?

### Follow-up Questions

- What happens if `os.RemoveAll` is called on a path that doesn't exist?
- How would you handle cleanup if the process is killed (SIGKILL) before defer runs?
- How does Kubernetes ephemeral storage interact with temp file accumulation?
- What happens if another goroutine holds a file handle in the temp dir during cleanup?
- How would you add disk usage metrics to detect cleanup failures over time?
- What's the risk of `/tmp` filling up if cleanup fails silently on many jobs?
- How would you implement a periodic cleanup sweep for orphaned temp dirs?

### Resume Talking Points

- Used `defer` pattern for guaranteed cleanup on all exit paths (success, error, panic)
- `os.RemoveAll` handles recursive deletion of source, transcoded outputs, and manifest
- Graceful error handling: permission errors logged as warnings, never crash the worker
- Nonexistent paths handled safely (`os.RemoveAll` returns nil for missing paths)
- Cleanup executes after S3 upload completes — no premature deletion of needed files


---

## Structured JSON Logging (zap)

### Interview Questions

- Why use structured logging (JSON) instead of plain text `log.Printf`?
- Why zap over other Go logging libraries (logrus, zerolog, slog)?
- How does structured logging improve observability in distributed systems?
- Why include pod_id in every log line — what problem does it solve?
- How does ISO 8601 timestamp format enable cross-service correlation?
- Why truncate ffmpeg_stderr to 500 chars instead of logging full output?
- What is the performance difference between zap and stdlib log?
- How do you ensure all error logs include consistent context fields?

### Follow-up Questions

- How would you add log levels (debug/info/warn/error) and control them at runtime?
- How would you correlate logs across API server and worker pod for the same job?
- How would you ship structured logs to a centralized system (ELK, CloudWatch, Datadog)?
- What's the tradeoff between sampling high-volume logs vs logging everything?
- How would you add request tracing (trace_id) that spans Kafka producer → consumer?
- How does log volume affect pod memory/CPU when processing thousands of jobs?
- Why not use Go 1.21's `slog` package instead of zap?
- How would you handle sensitive data in logs (S3 URIs, file names)?

### Resume Talking Points

- Replaced stdlib `log.Printf` with `go.uber.org/zap` for structured JSON output
- Every log line includes: timestamp (ISO 8601), job_id, pod_id (HOSTNAME), event_type
- Failure logs include: ffmpeg_stderr (truncated 500 chars), retry_count, error_type
- Custom `Logger` wrapper provides typed methods: LogJobEvent, LogJobError, LogTranscodeFailure
- Pod identity from HOSTNAME env var (Kubernetes downward API injection)
- Zero-allocation logging in hot path (zap's design principle)


---

## Worker Pod: Prometheus Metrics Emission

### Interview Questions

- Why use `_total` suffix on counters (e.g., `pulsegrid_job_completed_total`) — what naming convention does this follow?
- Why separate `pulsegrid_pod_resource_constrained` from `pulsegrid_transcode_failures_total` — can't constraint be a label?
- How does labeling `pulsegrid_transcode_failures_total` by `error_type` enable targeted alerting?
- Why expose worker metrics on port 8081 instead of the same port as the API?
- How does Prometheus histogram with `rendition` label enable per-rendition latency analysis?
- Why emit `PodResourceConstrained` before `os.Exit(1)` — is there a race with Prometheus scrape?
- How does the Prometheus pull model interact with short-lived worker pods?
- Why register metrics via `prometheus.Registerer` interface instead of global default registry?

### Follow-up Questions

- How would you handle metric cardinality explosion if rendition IDs are user-defined strings?
- What happens to metrics if a pod exits before Prometheus scrapes — are they lost?
- Would you use Prometheus Pushgateway for short-lived pods? Tradeoffs?
- How would you add SLO-based alerts (e.g., p99 transcode duration > 30 min)?
- How does KEDA's metrics server relate to the worker's Prometheus metrics?
- How would you test that metrics are emitted correctly without a real Prometheus server?
- What's the cost of histogram buckets on memory — why not use a Summary instead?
- How would you correlate a `pod_resource_constrained` event with Kubernetes node resource pressure?

### Resume Talking Points

- Followed Prometheus naming conventions: `_total` suffix on counters, `_seconds` on duration histograms
- Four worker metrics: `job_completed_total`, `transcode_failures_total` (labeled), `transcode_duration_seconds` (labeled), `pod_resource_constrained`
- `PodResourceConstrained` emitted just before `os.Exit(1)` — captures metric before pod dies
- Injectable `WorkerMetrics` struct with custom registry for test isolation
- Metrics endpoint on :8081 via stdlib `http.ServeMux` + `promhttp.Handler()`
- Histogram buckets tuned for transcode latency: 1s to 1800s (30 min max job duration)


---

## Worker Pod: Functional Testing & End-to-End Pipeline Validation

### Interview Questions

- Why mock S3 and Kafka at the interface level instead of using testcontainers?
- How do you test the full pipeline (consume → download → transcode → upload → metrics → commit) without real infrastructure?
- Why test handleJobOutcome separately from processJob — isn't it integration testing?
- How do you validate error classification drives correct routing (retry vs DLQ vs pod exit)?
- Why use a non-GroupID kafka.Reader in tests instead of mocking the commit interface?
- How do you verify Prometheus metrics are emitted with correct labels in unit tests?
- Why test manifest JSON validity separately from the upload flow?
- How do you validate S3 object tagging without hitting real AWS?
- What's the difference between testing the mock contract vs testing real S3 behavior?
- Why swap package-level variables (s3Downloader, kafkaProducer) with defer restore in tests?

### Follow-up Questions

- How would you add contract tests between producer (API) and consumer (Worker) for Kafka schema?
- When would you switch from interface mocks to testcontainers for integration tests?
- How do you prevent test pollution when multiple tests swap the same global variables?
- How would you test the full pipeline with real ffmpeg (using a tiny valid video file)?
- What are the risks of testing with `kafka.NewReader{Partition: 0}` vs a proper mock?
- How would you test graceful shutdown (SIGTERM during in-flight job)?
- How do you verify the cleanup path (temp dir removal) after both success and failure?
- What's the tradeoff of fast mock tests vs slow end-to-end tests with real Kafka/S3?

### Resume Talking Points

- Wrote 18 functional tests covering full worker pipeline with mocked external dependencies
- Validated all error classification paths: retryable (re-enqueue), permanent (DLQ), constraint (pod exit)
- Tested metrics emission with custom Prometheus registry — complete isolation between tests
- Verified S3 path structure ({jobID}/{rendition}/{filename}), tagging (job_id, completion_time, rendition), and URL encoding
- Manifest validation: valid JSON, all rendition IDs present, correct file sizes, RFC3339 timestamps
- Interface-based mock pattern: same S3Downloader/S3OutputUploader/KafkaProducer interfaces used in production
- Tests run in <1s without external dependencies (no Kafka broker, no S3, no ffmpeg needed for most tests)
