package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pulsegrid/pkg"
	"pulsegrid/pkg/store"
)

// DefaultListLimit and MaxListLimit bound the page size for GET /jobs.
const (
	DefaultListLimit = 100
	MaxListLimit     = 1000
)

// JobLister queries jobs matching a filter. Satisfied by *pkg/store.Store.
type JobLister interface {
	ListJobs(ctx context.Context, filter store.JobFilter) ([]store.JobSummary, int, error)
}

// JobsListResponse is returned by GET /jobs.
type JobsListResponse struct {
	Jobs   []JobListItem `json:"jobs"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// JobListItem is a single job summary in a GET /jobs response.
type JobListItem struct {
	JobID           string  `json:"job_id"`
	Status          string  `json:"status"`
	SubmissionTime  string  `json:"submission_time"`
	CompletionTime  *string `json:"completion_time,omitempty"`
	DurationSeconds *int    `json:"duration_seconds,omitempty"`
}

// JobsListHandler handles GET /jobs: queries jobs filtered by submission
// time range, status, and pagination.
type JobsListHandler struct {
	Store JobLister
}

// NewJobsListHandler returns a JobsListHandler wired to store.
func NewJobsListHandler(store JobLister) *JobsListHandler {
	return &JobsListHandler{Store: store}
}

func (h *JobsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	w.Header().Set("X-Request-Id", requestID)

	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", requestID)
		return
	}

	filter, verr := parseJobFilter(r.URL.Query())
	if verr != nil {
		writeError(w, verr.status, verr.code, verr.message, verr.detail, requestID)
		return
	}

	jobs, total, err := h.Store.ListJobs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query jobs", err.Error(), requestID)
		return
	}

	items := make([]JobListItem, len(jobs))
	for i, j := range jobs {
		item := JobListItem{
			JobID:          j.ID,
			Status:         string(j.Status),
			SubmissionTime: j.SubmissionTime.Format(time.RFC3339),
		}
		if j.CompletionTime != nil {
			t := j.CompletionTime.Format(time.RFC3339)
			item.CompletionTime = &t
			d := int(j.CompletionTime.Sub(j.SubmissionTime).Seconds())
			item.DurationSeconds = &d
		}
		items[i] = item
	}

	writeJSON(w, http.StatusOK, JobsListResponse{
		Jobs:   items,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
}

// parseJobFilter parses and validates GET /jobs query parameters:
// submitted_after, submitted_before (ISO 8601), status (comma-separated
// list), limit (default 100, max 1000), offset (default 0).
func parseJobFilter(q map[string][]string) (store.JobFilter, *validationError) {
	get := func(key string) string {
		if v, ok := q[key]; ok && len(v) > 0 {
			return v[0]
		}
		return ""
	}

	var filter store.JobFilter

	if v := get("submitted_after"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid submitted_after: must be ISO 8601", err.Error())
		}
		filter.SubmittedAfter = &t
	}

	if v := get("submitted_before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid submitted_before: must be ISO 8601", err.Error())
		}
		filter.SubmittedBefore = &t
	}

	if filter.SubmittedAfter != nil && filter.SubmittedBefore != nil && filter.SubmittedAfter.After(*filter.SubmittedBefore) {
		return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "submitted_after must not be after submitted_before", "")
	}

	if v := get("status"); v != "" {
		for _, s := range strings.Split(v, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			switch pkg.JobStatus(s) {
			case pkg.JobStatusSubmitting, pkg.JobStatusSubmitted, pkg.JobStatusProcessing, pkg.JobStatusCompleted, pkg.JobStatusFailed:
				filter.Statuses = append(filter.Statuses, pkg.JobStatus(s))
			default:
				return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid status: "+s, "")
			}
		}
	}

	filter.Limit = DefaultListLimit
	if v := get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid limit: must be a positive integer", "")
		}
		if n > MaxListLimit {
			return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid limit: must not exceed 1000", "")
		}
		filter.Limit = n
	}

	filter.Offset = 0
	if v := get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return store.JobFilter{}, newValidationError(http.StatusBadRequest, "VALIDATION_ERROR", "Invalid offset: must be a non-negative integer", "")
		}
		filter.Offset = n
	}

	return filter, nil
}
