// Command load-test drives the Pulsegrid API with synthetic upload traffic
// to validate latency SLOs and (where a Kubernetes API is reachable) worker
// autoscaling behavior.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"pulsegrid/pkg"
)

// Config controls a single load test run. Defaults are sized for the
// free-tier Terraform footprint in terraform/main.tf (a single t3.micro EKS
// node group, min/max_nodes 1/2) — NOT for the 500-job/50-pod scenario in
// requirements.md #16.1, which needs a node group sized well outside the
// free tier to have anywhere to scale to. Override every field via env var
// for a real-sized cluster.
type Config struct {
	APIBaseURL       string
	NumJobs          int
	VideoSizeBytes   int64
	BurstDuration    time.Duration
	TargetRenditions []pkg.Rendition
	PollInterval     time.Duration
	PollTimeout      time.Duration
	ReportPath       string
	SummaryPath      string

	// SLO targets, checked by BuildReport/RenderMarkdownSummary. Defaults
	// mirror requirements.md #15.1/#15.2.
	P50TargetSeconds  float64
	P99TargetSeconds  float64
	MinSuccessRatePct float64
}

// DefaultConfig returns a Config sized for a smoke-test run against the
// free-tier dev cluster: 10 small (10MB) jobs over a short burst, well
// within what a single t3.micro node can process without ever needing a
// second node to spin up.
func DefaultConfig() Config {
	return Config{
		APIBaseURL:     "http://localhost:8080",
		NumJobs:        10,
		VideoSizeBytes: 10 * 1024 * 1024, // 10MB, not requirements.md's 1GB reference size — kept small so a free-tier node can transcode it in seconds, not minutes
		BurstDuration:  10 * time.Second,
		TargetRenditions: []pkg.Rendition{
			{ID: "720p", Codec: "h264", BitrateKbps: 2500, Width: 1280, Height: 720},
			{ID: "480p", Codec: "h264", BitrateKbps: 1000, Width: 854, Height: 480},
		},
		PollInterval:      2 * time.Second,
		PollTimeout:       5 * time.Minute,
		ReportPath:        "load-test-report.json",
		SummaryPath:       "load-test-summary.md",
		P50TargetSeconds:  300,
		P99TargetSeconds:  1800,
		MinSuccessRatePct: 95,
	}
}

// ParseConfigFromEnv builds a Config from DefaultConfig, overriding any
// field whose env var (read via getenv) is set. getenv is injected so tests
// don't need to mutate process environment.
func ParseConfigFromEnv(getenv func(string) string) (Config, error) {
	cfg := DefaultConfig()

	if v := getenv("LOADTEST_API_BASE_URL"); v != "" {
		cfg.APIBaseURL = v
	}
	if v := getenv("LOADTEST_NUM_JOBS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_NUM_JOBS must be a positive integer, got %q", v)
		}
		cfg.NumJobs = n
	}
	if v := getenv("LOADTEST_VIDEO_SIZE_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_VIDEO_SIZE_BYTES must be a positive integer, got %q", v)
		}
		cfg.VideoSizeBytes = n
	}
	if v := getenv("LOADTEST_BURST_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_BURST_DURATION must be a positive duration, got %q", v)
		}
		cfg.BurstDuration = d
	}
	if v := getenv("LOADTEST_TARGET_RENDITIONS"); v != "" {
		renditions, err := parseRenditionNames(strings.Split(v, ","))
		if err != nil {
			return Config{}, err
		}
		cfg.TargetRenditions = renditions
	}
	if v := getenv("LOADTEST_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_POLL_INTERVAL must be a positive duration, got %q", v)
		}
		cfg.PollInterval = d
	}
	if v := getenv("LOADTEST_POLL_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_POLL_TIMEOUT must be a positive duration, got %q", v)
		}
		cfg.PollTimeout = d
	}
	if v := getenv("LOADTEST_REPORT_PATH"); v != "" {
		cfg.ReportPath = v
	}
	if v := getenv("LOADTEST_SUMMARY_PATH"); v != "" {
		cfg.SummaryPath = v
	}
	if v := getenv("LOADTEST_P50_TARGET_SECONDS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_P50_TARGET_SECONDS must be a positive number, got %q", v)
		}
		cfg.P50TargetSeconds = f
	}
	if v := getenv("LOADTEST_P99_TARGET_SECONDS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			return Config{}, fmt.Errorf("LOADTEST_P99_TARGET_SECONDS must be a positive number, got %q", v)
		}
		cfg.P99TargetSeconds = f
	}
	if v := getenv("LOADTEST_MIN_SUCCESS_RATE_PCT"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 100 {
			return Config{}, fmt.Errorf("LOADTEST_MIN_SUCCESS_RATE_PCT must be between 0 and 100, got %q", v)
		}
		cfg.MinSuccessRatePct = f
	}

	return cfg, nil
}

// renditionPresets maps short names accepted in LOADTEST_TARGET_RENDITIONS
// to full pkg.Rendition specs, matching the presets DefaultConfig uses.
var renditionPresets = map[string]pkg.Rendition{
	"720p": {ID: "720p", Codec: "h264", BitrateKbps: 2500, Width: 1280, Height: 720},
	"480p": {ID: "480p", Codec: "h264", BitrateKbps: 1000, Width: 854, Height: 480},
	"hls":  {ID: "hls", Codec: "h264", BitrateKbps: 1500, Width: 1280, Height: 720, HLS: true},
}

func parseRenditionNames(names []string) ([]pkg.Rendition, error) {
	renditions := make([]pkg.Rendition, 0, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		r, ok := renditionPresets[n]
		if !ok {
			return nil, fmt.Errorf("unknown rendition preset %q (known: 720p, 480p, hls)", n)
		}
		renditions = append(renditions, r)
	}
	if len(renditions) == 0 {
		return nil, fmt.Errorf("LOADTEST_TARGET_RENDITIONS must name at least one rendition")
	}
	return renditions, nil
}

// osGetenv adapts os.Getenv to the getenv func(string) string signature used
// by ParseConfigFromEnv, so main can call ParseConfigFromEnv(osGetenv).
func osGetenv(key string) string { return os.Getenv(key) }
