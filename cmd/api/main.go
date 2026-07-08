package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"pulsegrid/pkg"
)

// MaxUploadSize is 10GB file size limit (variable to allow test override).
var MaxUploadSize int64 = 10 * 1024 * 1024 * 1024 // 10 GB

// s3Uploader is the global S3 uploader. Nil when S3 not configured (local dev).
var s3Uploader pkg.S3Uploader

// kafkaProducer is the global Kafka producer. Nil when Kafka not configured (local dev).
var kafkaProducer pkg.KafkaProducer

// dbClient is the global Postgres client. Nil when DATABASE_URL not configured (local dev).
var dbClient pkg.DBClient

// apiMetrics holds Prometheus metrics for the API server.
var apiMetrics *pkg.Metrics

// ErrorResponse is the structured error response format.
type ErrorResponse struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`
	RequestID string `json:"request_id"`
	Timestamp string `json:"timestamp"`
	Detail    string `json:"detail,omitempty"`
}

// UploadResponse is the HTTP 202 success response.
type UploadResponse struct {
	JobID                    string `json:"job_id"`
	StatusURI                string `json:"status_uri"`
	EstimatedWaitTimeSeconds int    `json:"estimated_wait_time_seconds"`
	SubmissionTime           string `json:"submission_time"`
}

func main() {
	// Initialize S3 uploader if AWS configured (skip for local dev).
	if os.Getenv("AWS_REGION") != "" || os.Getenv("AWS_DEFAULT_REGION") != "" {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			log.Fatalf("failed to load AWS config: %v", err)
		}
		bucket := os.Getenv("PULSEGRID_SOURCE_BUCKET")
		if bucket == "" {
			bucket = "pulsegrid-source"
		}
		s3Uploader = pkg.NewS3Client(cfg, bucket)
		log.Printf("S3 uploader initialized (bucket: %s)", bucket)
	} else {
		log.Printf("S3 uploader not configured (no AWS_REGION). Running in local mode.")
	}

	// Initialize Postgres client if DATABASE_URL configured (skip for local dev).
	if os.Getenv("DATABASE_URL") != "" {
		db, err := pkg.NewPostgresClient(context.Background())
		if err != nil {
			log.Fatalf("failed to connect to postgres: %v", err)
		}
		dbClient = db
		log.Printf("Postgres client initialized")
	} else {
		log.Printf("Postgres not configured (no DATABASE_URL). Running in local mode.")
	}

	// Initialize Kafka producer if brokers configured (skip for local dev).
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		topic := os.Getenv("KAFKA_TOPIC")
		if topic == "" {
			topic = "transcoding-jobs"
		}
		dlqTopic := os.Getenv("KAFKA_DLQ_TOPIC")
		if dlqTopic == "" {
			dlqTopic = "transcoding-dlq"
		}
		kafkaProducer = pkg.NewKafkaClient(brokers, topic, dlqTopic)
		log.Printf("Kafka producer initialized (brokers: %s, topic: %s, dlq: %s)", brokersEnv, topic, dlqTopic)
	} else {
		log.Printf("Kafka producer not configured (no KAFKA_BROKERS). Running in local mode.")
	}

	// Initialize Prometheus metrics.
	apiMetrics = pkg.NewMetrics(prometheus.DefaultRegisterer)

	// Start queue depth poller if Kafka configured.
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		topic := os.Getenv("KAFKA_TOPIC")
		if topic == "" {
			topic = "transcoding-jobs"
		}
		consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
		if consumerGroup == "" {
			consumerGroup = "pulsegrid-workers"
		}
		pkg.StartQueueDepthPoller(context.Background(), pkg.QueueDepthPollerConfig{
			Brokers:       brokers,
			Topic:         topic,
			ConsumerGroup: consumerGroup,
			PollInterval:  30 * time.Second,
			Gauge:         apiMetrics.QueueDepthJobs,
		})
		log.Printf("Queue depth poller started (topic: %s, group: %s, interval: 30s)", topic, consumerGroup)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/videos/upload", handleVideoUpload)
	mux.HandleFunc("/jobs/", handleGetJob)
	mux.HandleFunc("/jobs", handleListJobs)
	mux.HandleFunc("/health", handleHealth)
	mux.Handle("/metrics", promhttp.Handler())

	addr := ":8080"
	log.Printf("pulsegrid api server starting on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// HealthResponse is the response for GET /health.
type HealthResponse struct {
	Status   string                   `json:"status"`
	Checks   map[string]ComponentCheck `json:"checks"`
}

// ComponentCheck is the health status of an individual component.
type ComponentCheck struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusBadRequest, "Method not allowed", "VALIDATION_ERROR", "Only GET is accepted")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := make(map[string]ComponentCheck)
	allHealthy := true

	// Check Postgres.
	if dbClient != nil {
		if err := dbClient.Ping(ctx); err != nil {
			checks["postgres"] = ComponentCheck{Status: "unhealthy", Error: err.Error()}
			allHealthy = false
		} else {
			checks["postgres"] = ComponentCheck{Status: "healthy"}
		}
	} else {
		checks["postgres"] = ComponentCheck{Status: "not_configured"}
	}

	// Check Kafka.
	if kafkaProducer != nil {
		if err := kafkaProducer.Ping(ctx); err != nil {
			checks["kafka"] = ComponentCheck{Status: "unhealthy", Error: err.Error()}
			allHealthy = false
		} else {
			checks["kafka"] = ComponentCheck{Status: "healthy"}
		}
	} else {
		checks["kafka"] = ComponentCheck{Status: "not_configured"}
	}

	// Check S3.
	if s3Uploader != nil {
		if err := s3Uploader.Ping(ctx); err != nil {
			checks["s3"] = ComponentCheck{Status: "unhealthy", Error: err.Error()}
			allHealthy = false
		} else {
			checks["s3"] = ComponentCheck{Status: "healthy"}
		}
	} else {
		checks["s3"] = ComponentCheck{Status: "not_configured"}
	}

	resp := HealthResponse{
		Checks: checks,
	}

	status := http.StatusOK
	if allHealthy {
		resp.Status = "healthy"
	} else {
		resp.Status = "unhealthy"
		status = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

func handleVideoUpload(w http.ResponseWriter, r *http.Request) {
	uploadStart := time.Now()

	if r.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, "Method not allowed", "VALIDATION_ERROR", "Only POST is accepted")
		return
	}

	// Generate request ID for tracing.
	requestID, err := pkg.GenerateJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to generate request ID", "INTERNAL_ERROR", "")
		return
	}

	// Enforce file size limit with MaxBytesReader.
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	// Parse multipart form. 32MB memory buffer, rest goes to temp files.
	err = r.ParseMultipartForm(32 << 20)
	if err != nil {
		if err.Error() == "http: request body too large" {
			writeErrorWithRequestID(w, http.StatusRequestEntityTooLarge, "File exceeds 10GB limit", "PAYLOAD_TOO_LARGE", requestID, "")
			return
		}
		writeErrorWithRequestID(w, http.StatusBadRequest, "Invalid multipart form data", "VALIDATION_ERROR", requestID, err.Error())
		return
	}

	// Extract source_name (required).
	sourceName := r.FormValue("source_name")
	if sourceName == "" {
		writeErrorWithRequestID(w, http.StatusBadRequest, "Missing required field: source_name", "VALIDATION_ERROR", requestID, "source_name is required")
		return
	}

	// Extract video file (required).
	file, header, err := r.FormFile("video")
	if err != nil {
		writeErrorWithRequestID(w, http.StatusBadRequest, "Missing required field: video", "VALIDATION_ERROR", requestID, "video file is required")
		return
	}
	defer file.Close()

	// Check file size from header (secondary check — MaxBytesReader is primary).
	if header.Size > MaxUploadSize {
		writeErrorWithRequestID(w, http.StatusRequestEntityTooLarge, "File exceeds 10GB limit", "PAYLOAD_TOO_LARGE", requestID,
			fmt.Sprintf("file size %d exceeds maximum %d bytes", header.Size, MaxUploadSize))
		return
	}

	// Extract and validate renditions JSON (optional — defaults applied if missing).
	renditions, err := parseRenditions(r.FormValue("renditions"))
	if err != nil {
		writeErrorWithRequestID(w, http.StatusBadRequest, "Invalid renditions JSON", "VALIDATION_ERROR", requestID, err.Error())
		return
	}

	// Generate Job ID.
	jobID, err := pkg.GenerateJobID()
	if err != nil {
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Failed to generate job ID", "INTERNAL_ERROR", requestID, "")
		return
	}

	submissionTime := time.Now().UTC()

	// Upload to S3 if configured. Skip in local dev mode.
	var sourceS3URI string
	if s3Uploader != nil {
		uri, err := s3Uploader.UploadSourceToS3(r.Context(), file, jobID, sourceName)
		if err != nil {
			writeErrorWithRequestID(w, http.StatusInternalServerError, "Failed to store video", "INTERNAL_ERROR", requestID, err.Error())
			return
		}
		sourceS3URI = uri
	} else {
		sourceS3URI = fmt.Sprintf("s3://pulsegrid-source/%s/original.mp4", jobID)
	}

	// Build Job struct.
	job := pkg.Job{
		JobID:                  jobID,
		SourceS3URI:            sourceS3URI,
		SourceFileName:         sourceName,
		SourceFileSizeBytes:    header.Size,
		Renditions:             renditions,
		OutputS3Prefix:         fmt.Sprintf("s3://pulsegrid-output/%s/", jobID),
		RetryCount:             0,
		MaxRetries:             3,
		Status:                 pkg.JobStatusSubmitting,
		SubmissionTime:         submissionTime,
		VisibilityTimeoutSecs:  1800,
	}

	// --- Atomic DB-Kafka write ordering (prevents orphans) ---
	// 1. Begin DB tx: INSERT job with status='submitting'
	// 2. Publish to Kafka
	// 3. UPDATE job status='submitted', COMMIT tx
	// If Kafka fails: rollback DB (job never existed from client view)
	// If DB commit fails after Kafka: log ALERT (orphan possible), still return 202

	if dbClient != nil {
		txHandle, err := dbClient.InsertJobTx(r.Context(), job)
		if err != nil {
			writeErrorWithRequestID(w, http.StatusInternalServerError, "Failed to record job", "INTERNAL_ERROR", requestID, err.Error())
			return
		}

		// Publish to Kafka (outside tx).
		if kafkaProducer != nil {
			if err := kafkaProducer.EnqueueJob(r.Context(), job); err != nil {
				// Kafka failed: rollback DB transaction — job never existed.
				txHandle.Rollback(r.Context())
				writeErrorWithRequestID(w, http.StatusInternalServerError, "Failed to queue job", "INTERNAL_ERROR", requestID, err.Error())
				return
			}
		}

		// Kafka succeeded: update status to 'submitted' and commit.
		if err := txHandle.UpdateStatusAndCommit(r.Context(), job.JobID, pkg.JobStatusSubmitted); err != nil {
			// CRITICAL: Job IS in Kafka queue but DB commit failed. Orphan possible.
			log.Printf("ALERT: DB commit failed after Kafka publish for job %s (request %s): %v — orphan in queue, operator must investigate", job.JobID, requestID, err)
			// Still return 202 — job IS in queue and will be processed.
		}

		// Record status event (best-effort).
		if err := dbClient.RecordStatusEvent(r.Context(), job.JobID, "submitted"); err != nil {
			log.Printf("WARN: failed to record status event for job %s: %v", job.JobID, err)
		}
	} else {
		// No DB configured (local dev mode): just enqueue to Kafka if available.
		if kafkaProducer != nil {
			if err := kafkaProducer.EnqueueJob(r.Context(), job); err != nil {
				writeErrorWithRequestID(w, http.StatusInternalServerError, "Failed to queue job", "INTERNAL_ERROR", requestID, err.Error())
				return
			}
		}
	}

	resp := UploadResponse{
		JobID:                    jobID,
		StatusURI:                fmt.Sprintf("/jobs/%s", jobID),
		EstimatedWaitTimeSeconds: 120,
		SubmissionTime:           submissionTime.Format(time.RFC3339),
	}

	// Emit Prometheus metrics on successful submission.
	if apiMetrics != nil {
		apiMetrics.JobsSubmittedTotal.Inc()
		apiMetrics.UploadDurationSeconds.Observe(time.Since(uploadStart).Seconds())
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}

// ListJobsResponse is the paginated response for GET /jobs.
type ListJobsResponse struct {
	Jobs   []pkg.JobSummary `json:"jobs"`
	Total  int              `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}

// validJobStatuses is the set of allowed status filter values.
var validJobStatuses = map[string]bool{
	"submitted":  true,
	"processing": true,
	"completed":  true,
	"failed":     true,
}

func handleListJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusBadRequest, "Method not allowed", "VALIDATION_ERROR", "Only GET is accepted")
		return
	}

	q := r.URL.Query()

	// Parse submitted_after.
	var submittedAfter *time.Time
	if val := q.Get("submitted_after"); val != "" {
		t, err := time.Parse(time.RFC3339, val)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid submitted_after timestamp", "VALIDATION_ERROR", "Must be ISO 8601 format (e.g. 2024-01-15T10:30:00Z)")
			return
		}
		submittedAfter = &t
	}

	// Parse submitted_before.
	var submittedBefore *time.Time
	if val := q.Get("submitted_before"); val != "" {
		t, err := time.Parse(time.RFC3339, val)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid submitted_before timestamp", "VALIDATION_ERROR", "Must be ISO 8601 format (e.g. 2024-01-15T10:30:00Z)")
			return
		}
		submittedBefore = &t
	}

	// Validate range: submitted_after must be before submitted_before.
	if submittedAfter != nil && submittedBefore != nil && submittedAfter.After(*submittedBefore) {
		writeError(w, http.StatusBadRequest, "submitted_after must be before submitted_before", "VALIDATION_ERROR", "")
		return
	}

	// Parse status filter.
	var statuses []pkg.JobStatus
	if val := q.Get("status"); val != "" {
		for _, s := range strings.Split(val, ",") {
			s = strings.TrimSpace(s)
			if !validJobStatuses[s] {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid status value: %s", s), "VALIDATION_ERROR", "Allowed: submitted, processing, completed, failed")
				return
			}
			statuses = append(statuses, pkg.JobStatus(s))
		}
	}

	// Parse limit.
	limit := 100
	if val := q.Get("limit"); val != "" {
		l, err := strconv.Atoi(val)
		if err != nil || l < 1 {
			writeError(w, http.StatusBadRequest, "Invalid limit parameter", "VALIDATION_ERROR", "Must be a positive integer")
			return
		}
		if l > 1000 {
			writeError(w, http.StatusBadRequest, "Limit exceeds maximum of 1000", "VALIDATION_ERROR", "Maximum allowed limit is 1000")
			return
		}
		limit = l
	}

	// Parse offset.
	offset := 0
	if val := q.Get("offset"); val != "" {
		o, err := strconv.Atoi(val)
		if err != nil || o < 0 {
			writeError(w, http.StatusBadRequest, "Invalid offset parameter", "VALIDATION_ERROR", "Must be a non-negative integer")
			return
		}
		offset = o
	}

	// Require DB client.
	if dbClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Database not configured", "SERVICE_UNAVAILABLE", "")
		return
	}

	filter := pkg.JobFilter{
		SubmittedAfter:  submittedAfter,
		SubmittedBefore: submittedBefore,
		Statuses:        statuses,
		Limit:           limit,
		Offset:          offset,
	}

	result, err := dbClient.QueryJobs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query jobs", "INTERNAL_ERROR", err.Error())
		return
	}

	resp := ListJobsResponse{
		Jobs:   result.Jobs,
		Total:  result.Total,
		Limit:  result.Limit,
		Offset: result.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// JobStatusResponse is the response for GET /jobs/{job_id}.
type JobStatusResponse struct {
	JobID          string            `json:"job_id"`
	Status         pkg.JobStatus     `json:"status"`
	SubmissionTime string            `json:"submission_time"`
	CompletionTime *string           `json:"completion_time,omitempty"`
	RetryCount     int               `json:"retry_count"`
	OutputFiles    []pkg.OutputFile  `json:"output_files,omitempty"`
	FailureReason  *string           `json:"failure_reason,omitempty"`
}

func handleGetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusBadRequest, "Method not allowed", "VALIDATION_ERROR", "Only GET is accepted")
		return
	}

	// Parse job_id from URL path: /jobs/{job_id}
	path := strings.TrimPrefix(r.URL.Path, "/jobs/")
	jobID := strings.TrimSpace(path)
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "Missing job_id in path", "VALIDATION_ERROR", "URL must be /jobs/{job_id}")
		return
	}

	if dbClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Database not configured", "SERVICE_UNAVAILABLE", "")
		return
	}

	job, err := dbClient.GetJobByID(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, pkg.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "Job not found", "NOT_FOUND", fmt.Sprintf("No job with id: %s", jobID))
			return
		}
		writeError(w, http.StatusInternalServerError, "Failed to query job", "INTERNAL_ERROR", err.Error())
		return
	}

	resp := JobStatusResponse{
		JobID:          job.JobID,
		Status:         job.Status,
		SubmissionTime: job.SubmissionTime.Format(time.RFC3339),
		RetryCount:     job.RetryCount,
		OutputFiles:    job.OutputFiles,
	}

	if job.CompletionTime != nil {
		ct := job.CompletionTime.Format(time.RFC3339)
		resp.CompletionTime = &ct
	}

	if job.FailureReason != "" {
		resp.FailureReason = &job.FailureReason
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// parseRenditions parses and validates the renditions JSON string.
// Returns default renditions if input is empty.
func parseRenditions(raw string) ([]pkg.Rendition, error) {
	if raw == "" {
		return defaultRenditions(), nil
	}

	var renditions []pkg.Rendition
	if err := json.Unmarshal([]byte(raw), &renditions); err != nil {
		return nil, fmt.Errorf("renditions must be valid JSON array: %w", err)
	}

	if len(renditions) == 0 {
		return nil, fmt.Errorf("renditions array must not be empty")
	}

	// Validate each rendition has at least an id or type field.
	for i, r := range renditions {
		if r.ID == "" && r.Type == "" {
			return nil, fmt.Errorf("rendition at index %d must have at least 'id' or 'type' field", i)
		}
	}

	return renditions, nil
}

// defaultRenditions returns the standard rendition set per design spec.
func defaultRenditions() []pkg.Rendition {
	return []pkg.Rendition{
		{ID: "720p", Resolution: "1280x720", VideoCodec: "libx264", VideoBitrate: "5M", AudioCodec: "aac", AudioBitrate: "128k"},
		{ID: "480p", Resolution: "854x480", VideoCodec: "libx264", VideoBitrate: "2.5M", AudioCodec: "aac", AudioBitrate: "96k"},
		{ID: "hls", Type: "hls_segments", SegmentDuration: 6, BaseResolution: "720p"},
	}
}

// writeError writes a structured error response.
func writeError(w http.ResponseWriter, status int, msg, code, detail string) {
	reqID, _ := pkg.GenerateJobID()
	writeErrorWithRequestID(w, status, msg, code, reqID, detail)
}

// writeErrorWithRequestID writes a structured error response with a known request ID.
func writeErrorWithRequestID(w http.ResponseWriter, status int, msg, code, requestID, detail string) {
	resp := ErrorResponse{
		Error:     msg,
		ErrorCode: code,
		RequestID: requestID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Detail:    detail,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}
