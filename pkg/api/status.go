package api

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"pulsegrid/pkg"
)

// JobGetter queries a job's stored metadata by id. Satisfied by
// *pkg/store.Store.
type JobGetter interface {
	GetJob(ctx context.Context, jobID string) (pkg.Job, error)
}

// ManifestFetcher retrieves a completed job's output file manifest.
// Satisfied by *pkg/storage.Downloader.
type ManifestFetcher interface {
	FetchManifest(ctx context.Context, jobID string) (pkg.Manifest, error)
}

// StatusResponse is returned by GET /jobs/{job_id}.
type StatusResponse struct {
	JobID          string           `json:"job_id"`
	Status         string           `json:"status"`
	SubmissionTime string           `json:"submission_time"`
	CompletionTime *string          `json:"completion_time,omitempty"`
	RetryCount     int              `json:"retry_count"`
	OutputFiles    []pkg.OutputFile `json:"output_files"`
	FailureReason  *string          `json:"failure_reason"`
}

// StatusHandler handles GET /jobs/{job_id}: queries job metadata, and for
// completed jobs fetches the output file list from the worker-generated
// manifest in S3.
type StatusHandler struct {
	Store     JobGetter
	Manifests ManifestFetcher
}

// NewStatusHandler returns a StatusHandler wired to store and manifests.
func NewStatusHandler(store JobGetter, manifests ManifestFetcher) *StatusHandler {
	return &StatusHandler{Store: store, Manifests: manifests}
}

func (h *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	w.Header().Set("X-Request-Id", requestID)

	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", requestID)
		return
	}

	jobID := r.PathValue("job_id")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "Missing job_id", "", requestID)
		return
	}

	ctx := r.Context()
	job, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Job not found", "", requestID)
			return
		}
		log.Printf("status job_id=%s request_id=%s event=db_query_failed error=%v", jobID, requestID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query job", err.Error(), requestID)
		return
	}

	resp := StatusResponse{
		JobID:          job.ID,
		Status:         string(job.Status),
		SubmissionTime: job.SubmissionTime.Format(time.RFC3339),
		RetryCount:     job.RetryCount,
		OutputFiles:    []pkg.OutputFile{},
		FailureReason:  job.FailureReason,
	}
	if job.CompletionTime != nil {
		t := job.CompletionTime.Format(time.RFC3339)
		resp.CompletionTime = &t
	}

	if job.Status == pkg.JobStatusCompleted && h.Manifests != nil {
		manifest, err := h.Manifests.FetchManifest(ctx, jobID)
		if err != nil {
			// The manifest is a best-effort enrichment: a completed job
			// always has a real status, but the manifest fetch is a second
			// system (S3) that can fail independently. Degrade to an empty
			// output_files list rather than failing the whole status query.
			log.Printf("status job_id=%s request_id=%s event=manifest_fetch_failed error=%v", jobID, requestID, err)
		} else {
			resp.OutputFiles = manifest.OutputFiles
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
