// Package api implements the Pulsegrid HTTP API server handlers.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"pulsegrid/pkg"
)

// DefaultMaxUploadBytes is the default maximum accepted video file size (10GB).
const DefaultMaxUploadBytes = 10 * 1024 * 1024 * 1024

// SourceUploader stores the uploaded source video and returns its S3 URI.
// Satisfied by *pkg/storage.Uploader.
type SourceUploader interface {
	UploadSource(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error)
}

// JobEnqueuer publishes a job to the transcoding job queue. Satisfied by
// *pkg/queue.Producer.
type JobEnqueuer interface {
	EnqueueJob(ctx context.Context, job pkg.Job) error
}

// JobStore persists and updates job metadata. Satisfied by *pkg/store.Store.
type JobStore interface {
	RecordJobMetadata(ctx context.Context, job pkg.Job) error
	UpdateJobStatus(ctx context.Context, jobID string, status pkg.JobStatus) error
	DeleteJob(ctx context.Context, jobID string) error
}

// UploadMetrics records Prometheus metrics for the upload handler. Satisfied
// by *pkg/metrics.Metrics.
type UploadMetrics interface {
	IncJobsSubmitted()
	ObserveUploadDuration(seconds float64)
}

// ErrorResponse is the structured error body returned for failed requests.
type ErrorResponse struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`
	RequestID string `json:"request_id"`
	Timestamp string `json:"timestamp"`
	Detail    string `json:"detail,omitempty"`
}

// UploadResponse is returned on successful job submission.
type UploadResponse struct {
	JobID                    string `json:"job_id"`
	StatusURI                string `json:"status_uri"`
	EstimatedWaitTimeSeconds int    `json:"estimated_wait_time_seconds"`
	SubmissionTime           string `json:"submission_time"`
}

// validationError carries the HTTP status/code for a request failure.
type validationError struct {
	status  int
	code    string
	message string
	detail  string
}

func (e *validationError) Error() string { return e.message }

func newValidationError(status int, code, message, detail string) *validationError {
	return &validationError{status: status, code: code, message: message, detail: detail}
}

// UploadHandler handles POST /videos/upload: parses and validates the
// multipart request, uploads the source video to S3, enqueues the
// transcoding job to Kafka, and records job metadata to Postgres.
type UploadHandler struct {
	MaxUploadBytes int64
	Uploader       SourceUploader
	Queue          JobEnqueuer
	Store          JobStore
	OutputBucket   string
	Metrics        UploadMetrics
}

// NewUploadHandler returns an UploadHandler wired to uploader, queue, and
// store, with the default upload size limit. outputBucket names the S3
// bucket transcoded outputs will eventually be written to. metrics may be
// nil, in which case no metrics are emitted.
func NewUploadHandler(uploader SourceUploader, queue JobEnqueuer, store JobStore, outputBucket string, metrics UploadMetrics) *UploadHandler {
	return &UploadHandler{
		MaxUploadBytes: DefaultMaxUploadBytes,
		Uploader:       uploader,
		Queue:          queue,
		Store:          store,
		OutputBucket:   outputBucket,
		Metrics:        metrics,
	}
}

func (h *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	w.Header().Set("X-Request-Id", requestID)
	start := time.Now()

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", requestID)
		return
	}

	videoFile, videoSize, sourceName, renditions, verr := h.parseAndValidate(r)
	if verr != nil {
		writeError(w, verr.status, verr.code, verr.message, verr.detail, requestID)
		return
	}
	defer func() {
		videoFile.Close()
		os.Remove(videoFile.Name())
	}()

	ctx := r.Context()

	jobID, err := pkg.NewJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate job id", err.Error(), requestID)
		return
	}

	sourceS3URI, err := h.Uploader.UploadSource(ctx, jobID, sourceName, videoFile)
	if err != nil {
		log.Printf("upload job_id=%s request_id=%s event=s3_upload_failed error=%v", jobID, requestID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to store video", err.Error(), requestID)
		return
	}

	job := pkg.Job{
		ID:                  jobID,
		SourceName:          sourceName,
		SourceFileSizeBytes: videoSize,
		SourceS3URI:         sourceS3URI,
		OutputS3Prefix:      fmt.Sprintf("s3://%s/%s/", h.OutputBucket, jobID),
		Renditions:          renditions,
		Status:              pkg.JobStatusSubmitting,
		RetryCount:          0,
		SubmissionTime:      time.Now().UTC(),
	}

	// Step 1: insert with status="submitting" before the job is visible in
	// the queue. If this fails, nothing downstream has happened yet.
	if err := h.Store.RecordJobMetadata(ctx, job); err != nil {
		log.Printf("upload job_id=%s request_id=%s event=db_insert_failed error=%v", jobID, requestID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to record job", err.Error(), requestID)
		return
	}

	// Step 2: publish to Kafka. If this fails, roll back the DB row so the
	// job never existed from the client's point of view.
	if err := h.Queue.EnqueueJob(ctx, job); err != nil {
		if delErr := h.Store.DeleteJob(ctx, job.ID); delErr != nil {
			log.Printf("ALERT job_id=%s request_id=%s event=orphan_row_rollback_failed kafka_error=%v db_error=%v — job row left in 'submitting' state, operator must investigate", jobID, requestID, err, delErr)
		}
		log.Printf("upload job_id=%s request_id=%s event=kafka_publish_failed error=%v", jobID, requestID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to queue job", err.Error(), requestID)
		return
	}

	// Step 3: mark the row confirmed in queue. If this fails, the job is
	// already in Kafka and will be processed; the DB just hasn't caught up.
	// This is not fatal to the client's request — log an alert for an
	// operator to reconcile instead of failing an already-queued job.
	if err := h.Store.UpdateJobStatus(ctx, job.ID, pkg.JobStatusSubmitted); err != nil {
		log.Printf("ALERT job_id=%s request_id=%s event=db_status_update_failed error=%v — job is in Kafka but DB still shows 'submitting', operator must investigate", jobID, requestID, err)
	}

	if h.Metrics != nil {
		h.Metrics.IncJobsSubmitted()
		h.Metrics.ObserveUploadDuration(time.Since(start).Seconds())
	}

	resp := UploadResponse{
		JobID:                    jobID,
		StatusURI:                fmt.Sprintf("/jobs/%s", jobID),
		EstimatedWaitTimeSeconds: 120,
		SubmissionTime:           job.SubmissionTime.Format(time.RFC3339),
	}
	writeJSON(w, http.StatusAccepted, resp)
}

// parseAndValidate streams the multipart request, spooling the video part to
// a temp file (so it can be re-read/seeked by the S3 uploader's retry logic)
// and extracting source_name and renditions. Callers own the returned file
// and must close and remove it.
func (h *UploadHandler) parseAndValidate(r *http.Request) (videoFile *os.File, videoSize int64, sourceName string, renditions []pkg.Rendition, verr *validationError) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid multipart/form-data request", err.Error())
	}

	var renditionsRaw []byte
	maxBytes := h.MaxUploadBytes

	cleanup := func() {
		if videoFile != nil {
			videoFile.Close()
			os.Remove(videoFile.Name())
			videoFile = nil
		}
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			cleanup()
			return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Malformed multipart request", err.Error())
		}

		switch part.FormName() {
		case "video":
			if part.FileName() == "" {
				part.Close()
				cleanup()
				return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: video", "")
			}

			tmp, tmpErr := os.CreateTemp("", "pulsegrid-upload-*.mp4")
			if tmpErr != nil {
				part.Close()
				return nil, 0, "", nil, newValidationError(http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to stage upload", tmpErr.Error())
			}

			n, copyErr := io.Copy(tmp, io.LimitReader(part, maxBytes+1))
			part.Close()
			if copyErr != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read video file", copyErr.Error())
			}
			if n > maxBytes {
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, 0, "", nil, newValidationError(http.StatusRequestEntityTooLarge, "VALIDATION_ERROR", fmt.Sprintf("File exceeds %d byte limit", maxBytes), "")
			}
			if _, err := tmp.Seek(0, io.SeekStart); err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, 0, "", nil, newValidationError(http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to stage upload", err.Error())
			}

			videoFile = tmp
			videoSize = n

		case "source_name":
			b, readErr := io.ReadAll(io.LimitReader(part, 4096))
			part.Close()
			if readErr != nil {
				cleanup()
				return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read source_name", readErr.Error())
			}
			sourceName = string(b)

		case "renditions":
			b, readErr := io.ReadAll(io.LimitReader(part, 1<<20))
			part.Close()
			if readErr != nil {
				cleanup()
				return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read renditions", readErr.Error())
			}
			renditionsRaw = b

		default:
			part.Close()
		}
	}

	if videoFile == nil {
		return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: video", "")
	}
	if sourceName == "" {
		cleanup()
		return nil, 0, "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: source_name", "field=source_name")
	}

	renditionsParsed, rverr := parseRenditions(renditionsRaw)
	if rverr != nil {
		cleanup()
		return nil, 0, "", nil, rverr
	}

	return videoFile, videoSize, sourceName, renditionsParsed, nil
}

// parseRenditions validates the optional renditions JSON field, falling back
// to the platform default rendition set when absent.
func parseRenditions(raw []byte) ([]pkg.Rendition, *validationError) {
	if len(raw) == 0 {
		return defaultRenditions(), nil
	}

	var renditions []pkg.Rendition
	if err := json.Unmarshal(raw, &renditions); err != nil {
		return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid rendition JSON", err.Error())
	}
	if len(renditions) == 0 {
		return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "renditions must contain at least one entry", "")
	}
	for _, r := range renditions {
		if r.ID == "" {
			return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid rendition: missing id", "")
		}
		if r.HLS {
			continue
		}
		if r.Codec == "" {
			return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Invalid rendition %q: missing codec", r.ID), "")
		}
		if r.BitrateKbps <= 0 {
			return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Invalid rendition %q: bitrate_kbps must be > 0", r.ID), "")
		}
		if r.Width <= 0 || r.Height <= 0 {
			return nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Invalid rendition %q: width/height must be > 0", r.ID), "")
		}
	}
	return renditions, nil
}

func defaultRenditions() []pkg.Rendition {
	return []pkg.Rendition{
		{ID: "720p", Codec: "libx264", BitrateKbps: 5000, Width: 1280, Height: 720},
		{ID: "480p", Codec: "libx264", BitrateKbps: 2500, Width: 854, Height: 480},
		{ID: "hls", HLS: true},
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message, detail, requestID string) {
	writeJSON(w, status, ErrorResponse{
		Error:     message,
		ErrorCode: code,
		RequestID: requestID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Detail:    detail,
	})
}

func newRequestID() string {
	id, err := pkg.NewJobID()
	if err != nil {
		return ""
	}
	return id
}
