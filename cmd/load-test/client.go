package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"pulsegrid/pkg"
)

// submitResponse mirrors pkg/api.UploadResponse without importing the api
// package (avoids pulling API-server-only dependencies into the harness
// binary).
type submitResponse struct {
	JobID          string `json:"job_id"`
	SubmissionTime string `json:"submission_time"`
}

// buildUploadRequest constructs the multipart/form-data POST body expected
// by pkg/api.UploadHandler: a "video" file part, a "source_name" field, and
// a "renditions" JSON field.
func buildUploadRequest(ctx context.Context, baseURL, sourceName string, videoSizeBytes int64, renditions []pkg.Rendition) (*http.Request, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	videoPart, err := w.CreateFormFile("video", sourceName)
	if err != nil {
		return nil, fmt.Errorf("create video part: %w", err)
	}
	if _, err := io.CopyN(videoPart, zeroReader{}, videoSizeBytes); err != nil {
		return nil, fmt.Errorf("write synthetic video body: %w", err)
	}

	if err := w.WriteField("source_name", sourceName); err != nil {
		return nil, fmt.Errorf("write source_name field: %w", err)
	}

	renditionsJSON, err := json.Marshal(renditions)
	if err != nil {
		return nil, fmt.Errorf("marshal renditions: %w", err)
	}
	if err := w.WriteField("renditions", string(renditionsJSON)); err != nil {
		return nil, fmt.Errorf("write renditions field: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/videos/upload", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

// zeroReader is an io.Reader producing an unbounded stream of zero bytes,
// used to synthesize video payloads of a configured size without holding
// the whole thing in memory.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// submitJob uploads one synthetic video and returns its job ID and the
// client-observed submission time (used as the latency clock start).
func submitJob(ctx context.Context, httpClient *http.Client, baseURL, sourceName string, videoSizeBytes int64, renditions []pkg.Rendition) (jobID string, submittedAt time.Time, err error) {
	req, err := buildUploadRequest(ctx, baseURL, sourceName, videoSizeBytes, renditions)
	if err != nil {
		return "", time.Time{}, err
	}

	submittedAt = time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", time.Time{}, fmt.Errorf("upload returned status %d: %s", resp.StatusCode, string(b))
	}

	var parsed submitResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", time.Time{}, fmt.Errorf("decode upload response: %w", err)
	}
	return parsed.JobID, submittedAt, nil
}

// statusResponse mirrors the subset of pkg/api.StatusResponse the harness
// needs to decide whether a job is done.
type statusResponse struct {
	Status        string  `json:"status"`
	FailureReason *string `json:"failure_reason"`
}

// jobResult records one submitted job's outcome for report generation.
type jobResult struct {
	JobID       string
	SubmittedAt time.Time
	CompletedAt time.Time
	Succeeded   bool
	Err         error
}

// pollJob polls GET /jobs/{id} at interval until the job reaches a terminal
// status ("completed" or "failed") or timeout elapses. Returns the terminal
// jobResult; a timeout is reported as a failed result rather than an error,
// since "job never finished" is itself a load-test signal worth reporting.
func pollJob(ctx context.Context, httpClient *http.Client, baseURL, jobID string, submittedAt time.Time, interval, timeout time.Duration) jobResult {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, failureReason, err := fetchStatus(ctx, httpClient, baseURL, jobID)
		if err == nil {
			switch status {
			case "completed":
				return jobResult{JobID: jobID, SubmittedAt: submittedAt, CompletedAt: time.Now(), Succeeded: true}
			case "failed":
				return jobResult{JobID: jobID, SubmittedAt: submittedAt, CompletedAt: time.Now(), Succeeded: false, Err: fmt.Errorf("job failed: %s", derefOrEmpty(failureReason))}
			}
		}

		if time.Now().After(deadline) {
			return jobResult{JobID: jobID, SubmittedAt: submittedAt, CompletedAt: time.Now(), Succeeded: false, Err: fmt.Errorf("timed out after %s waiting for terminal status", timeout)}
		}

		select {
		case <-ctx.Done():
			return jobResult{JobID: jobID, SubmittedAt: submittedAt, CompletedAt: time.Now(), Succeeded: false, Err: ctx.Err()}
		case <-ticker.C:
		}
	}
}

func fetchStatus(ctx context.Context, httpClient *http.Client, baseURL, jobID string) (status string, failureReason *string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/jobs/"+jobID, nil)
	if err != nil {
		return "", nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("status query returned %d", resp.StatusCode)
	}

	var parsed statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", nil, err
	}
	return parsed.Status, parsed.FailureReason, nil
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
