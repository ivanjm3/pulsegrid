package main

import (
	"testing"
	"time"
)

func getenvMap(m map[string]string) func(string) string {
	return func(key string) string { return m[key] }
}

func TestParseConfigFromEnv_Defaults(t *testing.T) {
	cfg, err := ParseConfigFromEnv(getenvMap(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	def := DefaultConfig()
	if cfg.NumJobs != def.NumJobs {
		t.Errorf("NumJobs = %d, want default %d", cfg.NumJobs, def.NumJobs)
	}
	if cfg.APIBaseURL != def.APIBaseURL {
		t.Errorf("APIBaseURL = %q, want default %q", cfg.APIBaseURL, def.APIBaseURL)
	}
}

func TestParseConfigFromEnv_Overrides(t *testing.T) {
	env := getenvMap(map[string]string{
		"LOADTEST_API_BASE_URL":         "http://api.internal:8080",
		"LOADTEST_NUM_JOBS":             "25",
		"LOADTEST_VIDEO_SIZE_BYTES":     "1048576",
		"LOADTEST_BURST_DURATION":       "30s",
		"LOADTEST_TARGET_RENDITIONS":    "720p,hls",
		"LOADTEST_POLL_INTERVAL":        "1s",
		"LOADTEST_POLL_TIMEOUT":         "1m",
		"LOADTEST_REPORT_PATH":          "/tmp/report.json",
		"LOADTEST_SUMMARY_PATH":         "/tmp/summary.md",
		"LOADTEST_P50_TARGET_SECONDS":   "60",
		"LOADTEST_P99_TARGET_SECONDS":   "300",
		"LOADTEST_MIN_SUCCESS_RATE_PCT": "99",
	})

	cfg, err := ParseConfigFromEnv(env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.APIBaseURL != "http://api.internal:8080" {
		t.Errorf("APIBaseURL = %q", cfg.APIBaseURL)
	}
	if cfg.NumJobs != 25 {
		t.Errorf("NumJobs = %d, want 25", cfg.NumJobs)
	}
	if cfg.VideoSizeBytes != 1048576 {
		t.Errorf("VideoSizeBytes = %d, want 1048576", cfg.VideoSizeBytes)
	}
	if cfg.BurstDuration != 30*time.Second {
		t.Errorf("BurstDuration = %v, want 30s", cfg.BurstDuration)
	}
	if len(cfg.TargetRenditions) != 2 || cfg.TargetRenditions[0].ID != "720p" || cfg.TargetRenditions[1].ID != "hls" {
		t.Errorf("TargetRenditions = %+v", cfg.TargetRenditions)
	}
	if cfg.PollInterval != time.Second {
		t.Errorf("PollInterval = %v, want 1s", cfg.PollInterval)
	}
	if cfg.PollTimeout != time.Minute {
		t.Errorf("PollTimeout = %v, want 1m", cfg.PollTimeout)
	}
	if cfg.ReportPath != "/tmp/report.json" {
		t.Errorf("ReportPath = %q", cfg.ReportPath)
	}
	if cfg.SummaryPath != "/tmp/summary.md" {
		t.Errorf("SummaryPath = %q", cfg.SummaryPath)
	}
	if cfg.P50TargetSeconds != 60 {
		t.Errorf("P50TargetSeconds = %v, want 60", cfg.P50TargetSeconds)
	}
	if cfg.P99TargetSeconds != 300 {
		t.Errorf("P99TargetSeconds = %v, want 300", cfg.P99TargetSeconds)
	}
	if cfg.MinSuccessRatePct != 99 {
		t.Errorf("MinSuccessRatePct = %v, want 99", cfg.MinSuccessRatePct)
	}
}

func TestParseConfigFromEnv_InvalidNumJobs(t *testing.T) {
	_, err := ParseConfigFromEnv(getenvMap(map[string]string{"LOADTEST_NUM_JOBS": "not-a-number"}))
	if err == nil {
		t.Fatal("expected error for invalid LOADTEST_NUM_JOBS")
	}

	_, err = ParseConfigFromEnv(getenvMap(map[string]string{"LOADTEST_NUM_JOBS": "0"}))
	if err == nil {
		t.Fatal("expected error for non-positive LOADTEST_NUM_JOBS")
	}
}

func TestParseConfigFromEnv_InvalidRendition(t *testing.T) {
	_, err := ParseConfigFromEnv(getenvMap(map[string]string{"LOADTEST_TARGET_RENDITIONS": "8k"}))
	if err == nil {
		t.Fatal("expected error for unknown rendition preset")
	}
}

func TestParseConfigFromEnv_InvalidSuccessRate(t *testing.T) {
	_, err := ParseConfigFromEnv(getenvMap(map[string]string{"LOADTEST_MIN_SUCCESS_RATE_PCT": "150"}))
	if err == nil {
		t.Fatal("expected error for out-of-range success rate")
	}
}
