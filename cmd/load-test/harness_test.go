package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pulsegrid/pkg"
)

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig([]string{
		"-base-url", "http://example.com/",
		"-num-jobs", "7",
		"-video-size", "12MB",
		"-burst-duration", "45s",
		"-target-renditions", "4",
		"-poll-interval", "2s",
		"-request-timeout", "10s",
		"-completion-timeout", "5m",
		"-output-dir", "out",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	if cfg.BaseURL != "http://example.com" {
		t.Fatalf("expected trimmed base url, got %q", cfg.BaseURL)
	}
	if cfg.MetricsURL != "http://example.com/metrics" {
		t.Fatalf("expected derived metrics url, got %q", cfg.MetricsURL)
	}
	if cfg.NumJobs != 7 || cfg.TargetRenditions != 4 {
		t.Fatalf("unexpected numeric config: %+v", cfg)
	}
	if cfg.VideoSizeBytes != 12*1024*1024 {
		t.Fatalf("unexpected video size bytes: %d", cfg.VideoSizeBytes)
	}
}

func TestBuildUploadRequest(t *testing.T) {
	videoPath := createTempFile(t, 128)
	req, err := buildUploadRequest(context.Background(), "http://loadtest.local", videoPath, "source-a", buildTargetRenditions(3))
	if err != nil {
		t.Fatalf("buildUploadRequest: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Fatalf("expected POST, got %s", req.Method)
	}
	if req.URL.String() != "http://loadtest.local/videos/upload" {
		t.Fatalf("unexpected url: %s", req.URL.String())
	}
	if !strings.Contains(req.Header.Get("Content-Type"), "multipart/form-data") {
		t.Fatalf("expected multipart content type, got %s", req.Header.Get("Content-Type"))
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse media type: %v", err)
	}
	r := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	fields := map[string]string{}
	parts := 0
	for {
		part, err := r.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read multipart: %v", err)
		}
		parts++
		value, _ := io.ReadAll(part)
		fields[part.FormName()] = string(value)
	}

	if fields["source_name"] != "source-a" {
		t.Fatalf("unexpected source_name: %q", fields["source_name"])
	}
	if fields["renditions"] == "" {
		t.Fatal("expected renditions field")
	}
	if parts < 3 {
		t.Fatalf("expected at least 3 multipart parts, got %d", parts)
	}
}

func TestPollJobUntilTerminal(t *testing.T) {
	jobID := "550e8400-e29b-41d4-a716-446655440000"
	submissionTime := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	completionTime := submissionTime.Add(2 * time.Minute)
	var requests int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/jobs/"):
			requests++
			w.Header().Set("Content-Type", "application/json")
			if requests == 1 {
				json.NewEncoder(w).Encode(map[string]any{
					"job_id":          jobID,
					"status":          string(pkg.JobStatusProcessing),
					"submission_time": submissionTime.Format(time.RFC3339),
				})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"job_id":          jobID,
				"status":          string(pkg.JobStatusCompleted),
				"submission_time": submissionTime.Format(time.RFC3339),
				"completion_time": completionTime.Format(time.RFC3339),
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := server.Client()
	cfg := Config{BaseURL: server.URL, PollInterval: 1 * time.Millisecond, CompletionTimeout: 2 * time.Second}
	results, err := pollJobsUntilTerminal(context.Background(), client, cfg, []JobSubmission{{JobID: jobID, SubmissionTime: submissionTime}})
	if err != nil {
		t.Fatalf("pollJobsUntilTerminal: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 outcome, got %d", len(results))
	}
	if results[0].Status != string(pkg.JobStatusCompleted) {
		t.Fatalf("expected completed status, got %s", results[0].Status)
	}
	if results[0].CompletionTime == nil {
		t.Fatal("expected completion time to be recorded")
	}
	if got := results[0].CompletionTime.Sub(results[0].SubmissionTime); got != 2*time.Minute {
		t.Fatalf("unexpected latency: %s", got)
	}
}

func TestBuildReportAndSummary(t *testing.T) {
	cfg := Config{
		BaseURL:          "http://example.com",
		MetricsURL:       "http://example.com/metrics",
		MinSuccessRate:   0.95,
		MaxP95Latency:    20 * time.Minute,
		MaxP99Latency:    30 * time.Minute,
		MaxScaleUpTime:   10 * time.Minute,
		MaxScaleDownTime: 15 * time.Minute,
	}
	completionA := time.Unix(1000, 0).UTC()
	completionB := time.Unix(1600, 0).UTC()
	report := buildReport(cfg, []JobOutcome{
		{JobID: "a", Status: string(pkg.JobStatusCompleted), SubmissionTime: time.Unix(0, 0).UTC(), CompletionTime: &completionA, Latency: completionA.Sub(time.Unix(0, 0).UTC())},
		{JobID: "b", Status: string(pkg.JobStatusFailed), SubmissionTime: time.Unix(0, 0).UTC(), CompletionTime: &completionB, FailureReason: "boom"},
	}, 4, ptrDuration(2*time.Minute), nil)

	if report.TotalJobs != 2 || report.Succeeded != 1 || report.Failed != 1 {
		t.Fatalf("unexpected report counters: %+v", report)
	}
	if report.ScalingEvents != 4 {
		t.Fatalf("unexpected scaling events: %d", report.ScalingEvents)
	}
	if report.Latencies.P50Seconds <= 0 {
		t.Fatalf("expected latency percentiles to be populated: %+v", report.Latencies)
	}

	summary := renderMarkdownSummary(report, cfg)
	for _, needle := range []string{"# Pulsegrid Load Test Summary", "| Success Rate |", "PASS", "FAIL"} {
		if !strings.Contains(summary, needle) {
			t.Fatalf("summary missing %q:\n%s", needle, summary)
		}
	}

	checks := evaluateSLOs(report, cfg)
	if len(checks) != 5 {
		t.Fatalf("expected 5 SLO checks, got %d", len(checks))
	}
}

func TestParseMetricsSample(t *testing.T) {
	sample := parseMetricsSample([]byte(`
# HELP pulsegrid_worker_pods_current Current worker pods
pulsegrid_worker_pods_current 4
pulsegrid_worker_pods_target 6
pulsegrid_scaling_events_total{direction="up"} 2
pulsegrid_scaling_events_total{direction="down"} 1
`))
	if sample == nil {
		t.Fatal("expected sample")
	}
	if sample.WorkerCurrent == nil || *sample.WorkerCurrent != 4 {
		t.Fatalf("unexpected current: %+v", sample.WorkerCurrent)
	}
	if sample.WorkerTarget == nil || *sample.WorkerTarget != 6 {
		t.Fatalf("unexpected target: %+v", sample.WorkerTarget)
	}
	if sample.ScalingEvents == nil || *sample.ScalingEvents != 3 {
		t.Fatalf("unexpected scaling events: %+v", sample.ScalingEvents)
	}
}

func createTempFile(t *testing.T, size int64) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "source-*.mp4")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if err := file.Truncate(size); err != nil {
		t.Fatalf("truncate temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return file.Name()
}

func ptrDuration(d time.Duration) *time.Duration {
	return &d
}

func TestWriteJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeJSONFile(path, map[string]any{"ok": true}); err != nil {
		t.Fatalf("writeJSONFile: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Contains(data, []byte(`"ok": true`)) {
		t.Fatalf("unexpected content: %s", data)
	}
}

func TestRunLoadTest_EndToEnd_100Jobs(t *testing.T) {
	var submitCount int32
	jobStates := struct {
		mu   sync.Mutex
		jobs map[string]int
	}{jobs: make(map[string]int)}

	metricsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "pulsegrid_worker_pods_current 2\n")
		_, _ = fmt.Fprintf(w, "pulsegrid_worker_pods_target 3\n")
		_, _ = fmt.Fprintf(w, "pulsegrid_scaling_events_total{direction=\"up\"} 1\n")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/videos/upload":
			idx := atomic.AddInt32(&submitCount, 1)
			jobID := fmt.Sprintf("job-%03d", idx)
			jobStates.mu.Lock()
			jobStates.jobs[jobID] = 0
			jobStates.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{
				"job_id":          jobID,
				"status_uri":      "/jobs/" + jobID,
				"submission_time": time.Now().UTC().Format(time.RFC3339),
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/jobs/"):
			jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
			jobStates.mu.Lock()
			state, ok := jobStates.jobs[jobID]
			if ok {
				jobStates.jobs[jobID] = state + 1
			}
			jobStates.mu.Unlock()

			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			if state == 0 {
				json.NewEncoder(w).Encode(map[string]any{
					"job_id":          jobID,
					"status":          string(pkg.JobStatusProcessing),
					"submission_time": time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339),
				})
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"job_id":          jobID,
				"status":          string(pkg.JobStatusCompleted),
				"submission_time": time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339),
				"completion_time": time.Now().UTC().Format(time.RFC3339),
			})
		case r.Method == http.MethodGet && r.URL.Path == "/metrics":
			metricsHandler(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		BaseURL:           server.URL,
		MetricsURL:        server.URL + "/metrics",
		NumJobs:           100,
		VideoSizeBytes:    64,
		BurstDuration:     5 * time.Millisecond,
		TargetRenditions:  3,
		PollInterval:      1 * time.Millisecond,
		RequestTimeout:    2 * time.Second,
		CompletionTimeout: 5 * time.Second,
		OutputDir:         t.TempDir(),
		MinSuccessRate:    0.95,
		MaxP95Latency:     20 * time.Minute,
		MaxP99Latency:     30 * time.Minute,
		MaxScaleUpTime:    10 * time.Minute,
		MaxScaleDownTime:  15 * time.Minute,
	}

	report, summary, err := RunLoadTest(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunLoadTest: %v", err)
	}
	if report.TotalJobs != 100 {
		t.Fatalf("expected 100 jobs, got %d", report.TotalJobs)
	}
	if report.Succeeded != 100 {
		t.Fatalf("expected 100 successful jobs, got %d", report.Succeeded)
	}
	if report.Failed != 0 {
		t.Fatalf("expected 0 failed jobs, got %d", report.Failed)
	}
	if report.SuccessRate < 0.99 {
		t.Fatalf("expected success rate near 100%%, got %.2f%%", report.SuccessRate*100)
	}
	if !strings.Contains(summary, "# Pulsegrid Load Test Summary") {
		t.Fatalf("summary missing title:\n%s", summary)
	}
	if !strings.Contains(summary, "PASS") {
		t.Fatalf("summary should contain PASS markers:\n%s", summary)
	}
}
