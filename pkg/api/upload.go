// Package api implements the Pulsegrid HTTP API server handlers.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"pulsegrid/pkg"
)

// DefaultMaxUploadBytes is the default maximum accepted video file size (10GB).
const DefaultMaxUploadBytes = 10 * 1024 * 1024 * 1024

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

// validationError carries the HTTP status/code for a request validation failure.
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

// UploadHandler handles POST /videos/upload: parses multipart form data and
// validates the request. S3/Kafka/Postgres wiring is added in later tasks.
type UploadHandler struct {
	MaxUploadBytes int64
}

// NewUploadHandler returns an UploadHandler configured with default limits.
func NewUploadHandler() *UploadHandler {
	return &UploadHandler{MaxUploadBytes: DefaultMaxUploadBytes}
}

func (h *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", requestID)
		return
	}

	_, _, verr := h.parseAndValidate(r)
	if verr != nil {
		writeError(w, verr.status, verr.code, verr.message, verr.detail, requestID)
		return
	}

	jobID, err := pkg.NewJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate job id", err.Error(), requestID)
		return
	}

	resp := UploadResponse{
		JobID:                    jobID,
		StatusURI:                fmt.Sprintf("/jobs/%s", jobID),
		EstimatedWaitTimeSeconds: 120,
		SubmissionTime:           time.Now().UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusAccepted, resp)
}

// parseAndValidate streams the multipart request, extracting source_name,
// renditions, and the video file, without buffering the file in memory.
func (h *UploadHandler) parseAndValidate(r *http.Request) (string, []pkg.Rendition, *validationError) {
	mr, err := r.MultipartReader()
	if err != nil {
		return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid multipart/form-data request", err.Error())
	}

	var (
		sourceName    string
		renditionsRaw []byte
		haveVideo     bool
	)

	maxBytes := h.MaxUploadBytes

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Malformed multipart request", err.Error())
		}

		switch part.FormName() {
		case "video":
			if part.FileName() == "" {
				part.Close()
				return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: video", "")
			}
			n, copyErr := io.Copy(io.Discard, io.LimitReader(part, maxBytes+1))
			part.Close()
			if copyErr != nil {
				return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read video file", copyErr.Error())
			}
			if n > maxBytes {
				return "", nil, newValidationError(http.StatusRequestEntityTooLarge, "VALIDATION_ERROR", fmt.Sprintf("File exceeds %d byte limit", maxBytes), "")
			}
			haveVideo = true

		case "source_name":
			b, readErr := io.ReadAll(io.LimitReader(part, 4096))
			part.Close()
			if readErr != nil {
				return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read source_name", readErr.Error())
			}
			sourceName = string(b)

		case "renditions":
			b, readErr := io.ReadAll(io.LimitReader(part, 1<<20))
			part.Close()
			if readErr != nil {
				return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Failed to read renditions", readErr.Error())
			}
			renditionsRaw = b

		default:
			part.Close()
		}
	}

	if !haveVideo {
		return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: video", "")
	}
	if sourceName == "" {
		return "", nil, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Missing field: source_name", "field=source_name")
	}

	renditions, verr := parseRenditions(renditionsRaw)
	if verr != nil {
		return "", nil, verr
	}

	return sourceName, renditions, nil
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
