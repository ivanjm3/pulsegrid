package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pulsegrid/pkg"
)

const (
	defaultBaseURL           = "http://localhost:8080"
	defaultMetricsPath       = "/metrics"
	defaultNumJobs           = 10
	defaultVideoSize         = "5MB"
	defaultBurstDuration     = 30 * time.Second
	defaultTargetRenditions  = 3
	defaultPollInterval      = 5 * time.Second
	defaultRequestTimeout    = 60 * time.Second
	defaultCompletionTimeout = 30 * time.Minute
	defaultOutputDir         = "load-test-results"
	defaultMinSuccessRate    = 0.95
	defaultP95Latency        = 20 * time.Minute
	defaultP99Latency        = 30 * time.Minute
	defaultScaleUpTime       = 10 * time.Minute
	defaultScaleDownTime     = 15 * time.Minute
)

// Config controls the load test run.
type Config struct {
	BaseURL           string
	MetricsURL        string
	NumJobs           int
	VideoSizeBytes    int64
	BurstDuration     time.Duration
	TargetRenditions  int
	PollInterval      time.Duration
	RequestTimeout    time.Duration
	CompletionTimeout time.Duration
	OutputDir         string
	MinSuccessRate    float64
	MaxP95Latency     time.Duration
	MaxP99Latency     time.Duration
	MaxScaleUpTime    time.Duration
	MaxScaleDownTime  time.Duration
}

// JobSubmission captures the API submission response.
type JobSubmission struct {
	JobID          string
	StatusURI      string
	SubmissionTime time.Time
	SubmittedAt    time.Time
	RequestTime    time.Duration
}

// JobOutcome captures the final terminal status for a job.
type JobOutcome struct {
	JobID          string        `json:"job_id"`
	Status         string        `json:"status"`
	SubmissionTime time.Time     `json:"submission_time"`
	CompletionTime *time.Time    `json:"completion_time,omitempty"`
	FailureReason  string        `json:"failure_reason,omitempty"`
	Latency        time.Duration `json:"latency"`
	PollCount      int           `json:"poll_count"`
}

// LoadTestReport is written to JSON for downstream analysis.
type LoadTestReport struct {
	GeneratedAt          time.Time     `json:"generated_at"`
	BaseURL              string        `json:"base_url"`
	MetricsURL           string        `json:"metrics_url,omitempty"`
	TotalJobs            int           `json:"total_jobs"`
	Succeeded            int           `json:"succeeded"`
	Failed               int           `json:"failed"`
	SuccessRate          float64       `json:"success_rate"`
	Latencies            LatencyReport `json:"latencies"`
	ScalingEvents        int64         `json:"scaling_events"`
	ScaleUpTimeSeconds   *float64      `json:"scale_up_time_seconds,omitempty"`
	ScaleDownTimeSeconds *float64      `json:"scale_down_time_seconds,omitempty"`
	Jobs                 []JobOutcome  `json:"jobs,omitempty"`
}

// LatencyReport captures percentile values in seconds.
type LatencyReport struct {
	P50Seconds float64 `json:"p50_seconds"`
	P95Seconds float64 `json:"p95_seconds"`
	P99Seconds float64 `json:"p99_seconds"`
}

// SLOCheck is rendered into the markdown summary.
type SLOCheck struct {
	Name       string
	Observed   string
	Threshold  string
	Status     string
	Applicable bool
}

// MetricsSample captures a single scrape of the metrics endpoint.
type MetricsSample struct {
	At            time.Time
	WorkerCurrent *float64
	WorkerTarget  *float64
	ScalingEvents *float64
}

// ParseConfig builds the harness configuration from CLI flags.
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("load-test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	baseURL := fs.String("base-url", defaultBaseURL, "API base URL")
	metricsURL := fs.String("metrics-url", "", "Prometheus metrics URL")
	numJobs := fs.Int("num-jobs", defaultNumJobs, "number of jobs to submit")
	videoSize := fs.String("video-size", defaultVideoSize, "video size such as 25MB or 1GB")
	burstDuration := fs.Duration("burst-duration", defaultBurstDuration, "total duration used to spread submissions")
	targetRenditions := fs.Int("target-renditions", defaultTargetRenditions, "number of rendition specs to send per job")
	pollInterval := fs.Duration("poll-interval", defaultPollInterval, "interval between polling rounds")
	requestTimeout := fs.Duration("request-timeout", defaultRequestTimeout, "timeout for each HTTP request")
	completionTimeout := fs.Duration("completion-timeout", defaultCompletionTimeout, "maximum time to wait for terminal job states")
	outputDir := fs.String("output-dir", defaultOutputDir, "directory for generated reports")
	minSuccessRate := fs.Float64("min-success-rate", defaultMinSuccessRate, "success rate threshold for SLO checks")
	maxP95Latency := fs.Duration("max-p95-latency", defaultP95Latency, "p95 latency threshold")
	maxP99Latency := fs.Duration("max-p99-latency", defaultP99Latency, "p99 latency threshold")
	maxScaleUpTime := fs.Duration("max-scale-up-time", defaultScaleUpTime, "scale-up latency threshold")
	maxScaleDownTime := fs.Duration("max-scale-down-time", defaultScaleDownTime, "scale-down latency threshold")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	videoSizeBytes, err := parseByteSize(*videoSize)
	if err != nil {
		return Config{}, err
	}

	if *numJobs <= 0 {
		return Config{}, fmt.Errorf("num-jobs must be > 0")
	}
	if *targetRenditions < 0 {
		return Config{}, fmt.Errorf("target-renditions must be >= 0")
	}
	if *pollInterval <= 0 {
		return Config{}, fmt.Errorf("poll-interval must be > 0")
	}
	if *requestTimeout <= 0 {
		return Config{}, fmt.Errorf("request-timeout must be > 0")
	}
	if *completionTimeout <= 0 {
		return Config{}, fmt.Errorf("completion-timeout must be > 0")
	}
	if *minSuccessRate < 0 || *minSuccessRate > 1 {
		return Config{}, fmt.Errorf("min-success-rate must be between 0 and 1")
	}

	cleanBaseURL := strings.TrimRight(*baseURL, "/")
	cleanMetricsURL := strings.TrimSpace(*metricsURL)
	if cleanMetricsURL == "" {
		cleanMetricsURL = cleanBaseURL + defaultMetricsPath
	}

	return Config{
		BaseURL:           cleanBaseURL,
		MetricsURL:        cleanMetricsURL,
		NumJobs:           *numJobs,
		VideoSizeBytes:    videoSizeBytes,
		BurstDuration:     *burstDuration,
		TargetRenditions:  *targetRenditions,
		PollInterval:      *pollInterval,
		RequestTimeout:    *requestTimeout,
		CompletionTimeout: *completionTimeout,
		OutputDir:         *outputDir,
		MinSuccessRate:    *minSuccessRate,
		MaxP95Latency:     *maxP95Latency,
		MaxP99Latency:     *maxP99Latency,
		MaxScaleUpTime:    *maxScaleUpTime,
		MaxScaleDownTime:  *maxScaleDownTime,
	}, nil
}

// RunLoadTest performs the submit/poll/measure/report workflow.
func RunLoadTest(ctx context.Context, cfg Config) (LoadTestReport, string, error) {
	if cfg.NumJobs <= 0 {
		return LoadTestReport{}, "", fmt.Errorf("num-jobs must be > 0")
	}

	sourcePath, cleanupSource, err := createSourceFixture(cfg.VideoSizeBytes)
	if err != nil {
		return LoadTestReport{}, "", err
	}
	defer cleanupSource()

	client := &http.Client{}
	metricsSampler := newMetricsSampler(cfg.MetricsURL, cfg.PollInterval)
	metricsCtx, cancelMetrics := context.WithCancel(ctx)
	defer cancelMetrics()
	metricsSampler.Start(metricsCtx, client)

	loadStart := time.Now().UTC()
	var submissions []JobSubmission
	for i := 0; i < cfg.NumJobs; i++ {
		jobCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		submission, submitErr := submitJob(jobCtx, client, cfg, sourcePath, i)
		cancel()
		if submitErr != nil {
			return LoadTestReport{}, "", submitErr
		}
		submissions = append(submissions, submission)

		if i < cfg.NumJobs-1 && cfg.BurstDuration > 0 {
			sleepFor := cfg.BurstDuration / time.Duration(cfg.NumJobs)
			if sleepFor > 0 {
				select {
				case <-ctx.Done():
					return LoadTestReport{}, "", ctx.Err()
				case <-time.After(sleepFor):
				}
			}
		}
	}

	results, err := pollJobsUntilTerminal(ctx, client, cfg, submissions)
	if err != nil {
		return LoadTestReport{}, "", err
	}

	metricsSampler.Stop()
	samples := metricsSampler.Samples()
	scalingEvents, scaleUp, scaleDown := deriveScalingDurations(samples, loadStart, time.Now().UTC())

	report := buildReport(cfg, results, scalingEvents, scaleUp, scaleDown)
	summary := renderMarkdownSummary(report, cfg)
	return report, summary, nil
}

func createSourceFixture(sizeBytes int64) (string, func(), error) {
	if sizeBytes < 0 {
		return "", func() {}, fmt.Errorf("video size must be >= 0")
	}

	file, err := os.CreateTemp("", "pulsegrid-loadtest-*.mp4")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}

	if err := file.Truncate(sizeBytes); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", func() {}, err
	}

	return file.Name(), cleanup, nil
}

func submitJob(ctx context.Context, client *http.Client, cfg Config, sourcePath string, index int) (JobSubmission, error) {
	req, err := buildUploadRequest(ctx, cfg.BaseURL, sourcePath, fmt.Sprintf("load-test-%03d", index+1), buildTargetRenditions(cfg.TargetRenditions))
	if err != nil {
		return JobSubmission{}, err
	}

	start := time.Now().UTC()
	resp, err := client.Do(req)
	if err != nil {
		return JobSubmission{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return JobSubmission{}, fmt.Errorf("submit job failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		JobID          string `json:"job_id"`
		StatusURI      string `json:"status_uri"`
		SubmissionTime string `json:"submission_time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return JobSubmission{}, err
	}

	submissionTime, err := time.Parse(time.RFC3339, parsed.SubmissionTime)
	if err != nil {
		return JobSubmission{}, err
	}

	return JobSubmission{
		JobID:          parsed.JobID,
		StatusURI:      parsed.StatusURI,
		SubmissionTime: submissionTime,
		SubmittedAt:    start,
		RequestTime:    time.Since(start),
	}, nil
}

func buildUploadRequest(ctx context.Context, baseURL, sourcePath, sourceName string, renditions []pkg.Rendition) (*http.Request, error) {
	file, err := os.Open(sourcePath)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		defer file.Close()

		if err := mw.WriteField("source_name", sourceName); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		payload, err := json.Marshal(renditions)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if err := mw.WriteField("renditions", string(payload)); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		part, err := mw.CreateFormFile("video", filepath.Base(sourcePath))
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(part, file); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		if err := mw.Close(); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		if err := pw.Close(); err != nil {
			return
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/videos/upload", pr)
	if err != nil {
		_ = pw.CloseWithError(err)
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req, nil
}

func pollJobsUntilTerminal(ctx context.Context, client *http.Client, cfg Config, submissions []JobSubmission) ([]JobOutcome, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, cfg.CompletionTimeout)
	defer cancel()

	outcomes := make([]JobOutcome, 0, len(submissions))
	pending := make(map[string]JobSubmission, len(submissions))
	for _, submission := range submissions {
		pending[submission.JobID] = submission
	}

	for len(pending) > 0 {
		progressed := false
		for jobID, submission := range pending {
			outcome, terminal, err := pollSingleJob(deadlineCtx, client, cfg.BaseURL, submission, jobID)
			if err != nil {
				return nil, err
			}
			if terminal {
				outcomes = append(outcomes, outcome)
				delete(pending, jobID)
				progressed = true
			}
		}

		if len(pending) == 0 {
			break
		}
		if !progressed {
			select {
			case <-deadlineCtx.Done():
				return nil, deadlineCtx.Err()
			case <-time.After(cfg.PollInterval):
			}
		}
	}

	sort.Slice(outcomes, func(i, j int) bool {
		return outcomes[i].JobID < outcomes[j].JobID
	})
	return outcomes, nil
}

func pollSingleJob(ctx context.Context, client *http.Client, baseURL string, submission JobSubmission, jobID string) (JobOutcome, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/jobs/"+jobID, nil)
	if err != nil {
		return JobOutcome{}, false, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return JobOutcome{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return JobOutcome{}, false, fmt.Errorf("job %s not found", jobID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return JobOutcome{}, false, fmt.Errorf("poll job failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		JobID          string           `json:"job_id"`
		Status         string           `json:"status"`
		SubmissionTime string           `json:"submission_time"`
		CompletionTime *string          `json:"completion_time,omitempty"`
		FailureReason  *string          `json:"failure_reason,omitempty"`
		OutputFiles    []pkg.OutputFile `json:"output_files,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return JobOutcome{}, false, err
	}

	submissionTime, err := time.Parse(time.RFC3339, parsed.SubmissionTime)
	if err != nil {
		return JobOutcome{}, false, err
	}

	outcome := JobOutcome{
		JobID:          parsed.JobID,
		Status:         parsed.Status,
		SubmissionTime: submissionTime,
		PollCount:      1,
	}

	if parsed.CompletionTime != nil {
		completionTime, parseErr := time.Parse(time.RFC3339, *parsed.CompletionTime)
		if parseErr != nil {
			return JobOutcome{}, false, parseErr
		}
		outcome.CompletionTime = &completionTime
		outcome.Latency = completionTime.Sub(submissionTime)
	} else if parsed.Status == string(pkg.JobStatusCompleted) || parsed.Status == string(pkg.JobStatusFailed) {
		now := time.Now().UTC()
		outcome.CompletionTime = &now
		outcome.Latency = now.Sub(submissionTime)
	}

	if parsed.FailureReason != nil {
		outcome.FailureReason = *parsed.FailureReason
	}

	switch pkg.JobStatus(parsed.Status) {
	case pkg.JobStatusCompleted:
		return outcome, true, nil
	case pkg.JobStatusFailed:
		return outcome, true, nil
	default:
		return outcome, false, nil
	}
}

func buildReport(cfg Config, outcomes []JobOutcome, scalingEvents int64, scaleUp, scaleDown *time.Duration) LoadTestReport {
	succeeded := 0
	latencies := make([]float64, 0, len(outcomes))
	for i := range outcomes {
		if outcomes[i].Status == string(pkg.JobStatusCompleted) {
			succeeded++
			latencies = append(latencies, outcomes[i].Latency.Seconds())
		}
	}

	failed := len(outcomes) - succeeded
	successRate := 0.0
	if len(outcomes) > 0 {
		successRate = float64(succeeded) / float64(len(outcomes))
	}

	sort.Float64s(latencies)
	report := LoadTestReport{
		GeneratedAt: time.Now().UTC(),
		BaseURL:     cfg.BaseURL,
		MetricsURL:  cfg.MetricsURL,
		TotalJobs:   len(outcomes),
		Succeeded:   succeeded,
		Failed:      failed,
		SuccessRate: successRate,
		Latencies: LatencyReport{
			P50Seconds: percentile(latencies, 0.50),
			P95Seconds: percentile(latencies, 0.95),
			P99Seconds: percentile(latencies, 0.99),
		},
		ScalingEvents: scalingEvents,
		Jobs:          append([]JobOutcome(nil), outcomes...),
	}

	if scaleUp != nil {
		value := scaleUp.Seconds()
		report.ScaleUpTimeSeconds = &value
	}
	if scaleDown != nil {
		value := scaleDown.Seconds()
		report.ScaleDownTimeSeconds = &value
	}

	return report
}

func renderMarkdownSummary(report LoadTestReport, cfg Config) string {
	checks := evaluateSLOs(report, cfg)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# Pulsegrid Load Test Summary\n\n")
	fmt.Fprintf(&buf, "- Generated at: %s\n", report.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&buf, "- Base URL: %s\n", report.BaseURL)
	fmt.Fprintf(&buf, "- Total jobs: %d\n", report.TotalJobs)
	fmt.Fprintf(&buf, "- Succeeded: %d\n", report.Succeeded)
	fmt.Fprintf(&buf, "- Failed: %d\n", report.Failed)
	fmt.Fprintf(&buf, "- Success rate: %.2f%%\n", report.SuccessRate*100)
	fmt.Fprintf(&buf, "- Scaling events: %d\n\n", report.ScalingEvents)

	fmt.Fprintf(&buf, "## Latency\n\n")
	fmt.Fprintf(&buf, "- p50: %.2fs\n", report.Latencies.P50Seconds)
	fmt.Fprintf(&buf, "- p95: %.2fs\n", report.Latencies.P95Seconds)
	fmt.Fprintf(&buf, "- p99: %.2fs\n", report.Latencies.P99Seconds)
	if report.ScaleUpTimeSeconds != nil {
		fmt.Fprintf(&buf, "- scale-up observed: %.2fs\n", *report.ScaleUpTimeSeconds)
	}
	if report.ScaleDownTimeSeconds != nil {
		fmt.Fprintf(&buf, "- scale-down observed: %.2fs\n", *report.ScaleDownTimeSeconds)
	}

	fmt.Fprintf(&buf, "\n## SLO Checks\n\n")
	fmt.Fprintf(&buf, "| Check | Observed | Threshold | Status |\n")
	fmt.Fprintf(&buf, "|---|---:|---:|---|\n")
	for _, check := range checks {
		if !check.Applicable {
			fmt.Fprintf(&buf, "| %s | %s | %s | N/A |\n", check.Name, check.Observed, check.Threshold)
			continue
		}
		fmt.Fprintf(&buf, "| %s | %s | %s | %s |\n", check.Name, check.Observed, check.Threshold, check.Status)
	}

	return buf.String()
}

func evaluateSLOs(report LoadTestReport, cfg Config) []SLOCheck {
	checks := []SLOCheck{
		{
			Name:       "Success Rate",
			Observed:   fmt.Sprintf("%.2f%%", report.SuccessRate*100),
			Threshold:  fmt.Sprintf("%.2f%%", cfg.MinSuccessRate*100),
			Status:     passFail(report.SuccessRate >= cfg.MinSuccessRate),
			Applicable: true,
		},
		{
			Name:       "p95 Latency",
			Observed:   durationOrNA(report.Latencies.P95Seconds),
			Threshold:  cfg.MaxP95Latency.String(),
			Status:     passFail(report.Latencies.P95Seconds <= cfg.MaxP95Latency.Seconds()),
			Applicable: true,
		},
		{
			Name:       "p99 Latency",
			Observed:   durationOrNA(report.Latencies.P99Seconds),
			Threshold:  cfg.MaxP99Latency.String(),
			Status:     passFail(report.Latencies.P99Seconds <= cfg.MaxP99Latency.Seconds()),
			Applicable: true,
		},
		{
			Name:       "Scale-up Time",
			Threshold:  cfg.MaxScaleUpTime.String(),
			Applicable: report.ScaleUpTimeSeconds != nil,
		},
		{
			Name:       "Scale-down Time",
			Threshold:  cfg.MaxScaleDownTime.String(),
			Applicable: report.ScaleDownTimeSeconds != nil,
		},
	}

	if report.ScaleUpTimeSeconds != nil {
		checks[3].Observed = durationOrNA(*report.ScaleUpTimeSeconds)
		checks[3].Status = passFail(*report.ScaleUpTimeSeconds <= cfg.MaxScaleUpTime.Seconds())
	}
	if report.ScaleDownTimeSeconds != nil {
		checks[4].Observed = durationOrNA(*report.ScaleDownTimeSeconds)
		checks[4].Status = passFail(*report.ScaleDownTimeSeconds <= cfg.MaxScaleDownTime.Seconds())
	}

	return checks
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func durationOrNA(seconds float64) string {
	if seconds <= 0 {
		return "N/A"
	}
	return (time.Duration(seconds * float64(time.Second))).String()
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	idx := int(float64(len(values)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func parseByteSize(value string) (int64, error) {
	trimmed := strings.TrimSpace(strings.ToUpper(value))
	if trimmed == "" {
		return 0, fmt.Errorf("video size cannot be empty")
	}

	multiplier := int64(1)
	switch {
	case strings.HasSuffix(trimmed, "KB"):
		multiplier = 1024
		trimmed = strings.TrimSuffix(trimmed, "KB")
	case strings.HasSuffix(trimmed, "MB"):
		multiplier = 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "MB")
	case strings.HasSuffix(trimmed, "GB"):
		multiplier = 1024 * 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "GB")
	case strings.HasSuffix(trimmed, "B"):
		trimmed = strings.TrimSuffix(trimmed, "B")
	}

	amount, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
	if err != nil {
		return 0, fmt.Errorf("parse video size: %w", err)
	}
	if amount < 0 {
		return 0, fmt.Errorf("video size must be >= 0")
	}
	return int64(amount * float64(multiplier)), nil
}

func buildTargetRenditions(count int) []pkg.Rendition {
	if count <= 0 {
		return nil
	}

	defaults := []pkg.Rendition{
		{ID: "720p", Resolution: "1280x720", VideoCodec: "libx264", VideoBitrate: "5M", AudioCodec: "aac", AudioBitrate: "128k"},
		{ID: "480p", Resolution: "854x480", VideoCodec: "libx264", VideoBitrate: "2.5M", AudioCodec: "aac", AudioBitrate: "96k"},
		{ID: "hls", Type: "hls_segments", SegmentDuration: 6, BaseResolution: "720p"},
		{ID: "360p", Resolution: "640x360", VideoCodec: "libx264", VideoBitrate: "1.5M", AudioCodec: "aac", AudioBitrate: "96k"},
	}

	renditions := make([]pkg.Rendition, 0, count)
	for i := 0; i < count; i++ {
		if i < len(defaults) {
			renditions = append(renditions, defaults[i])
			continue
		}
		idx := i + 1
		renditions = append(renditions, pkg.Rendition{
			ID:           fmt.Sprintf("rendition-%d", idx),
			Resolution:   "1280x720",
			VideoCodec:   "libx264",
			VideoBitrate: "5M",
			AudioCodec:   "aac",
			AudioBitrate: "128k",
		})
	}

	return renditions
}

type metricsSampler struct {
	url      string
	interval time.Duration

	mu       sync.Mutex
	samples  []MetricsSample
	stopOnce sync.Once
	stop     chan struct{}
}

func newMetricsSampler(url string, interval time.Duration) *metricsSampler {
	return &metricsSampler{url: url, interval: interval, stop: make(chan struct{})}
}

func (m *metricsSampler) Start(ctx context.Context, client *http.Client) {
	if m.url == "" || m.interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		m.scrape(ctx, client)
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stop:
				return
			case <-ticker.C:
				m.scrape(ctx, client)
			}
		}
	}()
}

func (m *metricsSampler) scrape(ctx context.Context, client *http.Client) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url, nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return
	}
	sample := parseMetricsSample(data)
	if sample == nil {
		return
	}
	sample.At = time.Now().UTC()
	m.mu.Lock()
	m.samples = append(m.samples, *sample)
	m.mu.Unlock()
}

func (m *metricsSampler) Stop() {
	m.stopOnce.Do(func() {
		close(m.stop)
	})
}

func (m *metricsSampler) Samples() []MetricsSample {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]MetricsSample(nil), m.samples...)
}

func parseMetricsSample(data []byte) *MetricsSample {
	var sample MetricsSample
	seen := false

	for _, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}

		switch {
		case strings.HasPrefix(line, "pulsegrid_worker_pods_current"):
			sample.WorkerCurrent = floatPtr(value)
			seen = true
		case strings.HasPrefix(line, "pulsegrid_worker_pods_target"):
			sample.WorkerTarget = floatPtr(value)
			seen = true
		case strings.HasPrefix(line, "pulsegrid_scaling_events_total"):
			if sample.ScalingEvents == nil {
				sample.ScalingEvents = floatPtr(0)
			}
			*sample.ScalingEvents += value
			seen = true
		}
	}

	if !seen {
		return nil
	}
	return &sample
}

func deriveScalingDurations(samples []MetricsSample, loadStart, loadEnd time.Time) (int64, *time.Duration, *time.Duration) {
	if len(samples) == 0 {
		return 0, nil, nil
	}

	var events int64
	for _, sample := range samples {
		if sample.ScalingEvents != nil {
			events = int64(*sample.ScalingEvents)
		}
	}

	var scaleUp *time.Duration
	var scaleDown *time.Duration
	var scaleUpStart time.Time
	var scaleDownStart time.Time
	var sawScaleUpStart bool
	var sawScaleDownStart bool

	for _, sample := range samples {
		if sample.WorkerCurrent == nil || sample.WorkerTarget == nil {
			continue
		}
		current := *sample.WorkerCurrent
		target := *sample.WorkerTarget

		if !sawScaleUpStart && target > 0 && current < target {
			sawScaleUpStart = true
			scaleUpStart = sample.At
		}
		if sawScaleUpStart && scaleUp == nil && current >= target && target > 0 {
			value := sample.At.Sub(scaleUpStart)
			scaleUp = &value
		}

		if sample.At.After(loadEnd) {
			if !sawScaleDownStart && current > 0 {
				sawScaleDownStart = true
				scaleDownStart = loadEnd
			}
			if sawScaleDownStart && scaleDown == nil && current <= target {
				value := sample.At.Sub(scaleDownStart)
				scaleDown = &value
			}
		}
	}

	if scaleUp != nil && *scaleUp < 0 {
		zero := time.Duration(0)
		scaleUp = &zero
	}
	if scaleDown != nil && *scaleDown < 0 {
		zero := time.Duration(0)
		scaleDown = &zero
	}

	return events, scaleUp, scaleDown
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func floatPtr(v float64) *float64 {
	return &v
}

var _ = errors.New
