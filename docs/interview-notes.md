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
