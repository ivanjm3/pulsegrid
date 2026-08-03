package main

import (
	"strings"
	"testing"
	"time"
)

func TestBuildReport_CountsAndLatencies(t *testing.T) {
	base := time.Now()
	results := []jobResult{
		{SubmittedAt: base, CompletedAt: base.Add(10 * time.Second), Succeeded: true},
		{SubmittedAt: base, CompletedAt: base.Add(20 * time.Second), Succeeded: true},
		{SubmittedAt: base, CompletedAt: base.Add(30 * time.Second), Succeeded: true},
		{SubmittedAt: base, CompletedAt: base.Add(5 * time.Second), Succeeded: false},
	}

	report := BuildReport(results, nil)

	if report.TotalJobs != 4 {
		t.Errorf("TotalJobs = %d, want 4", report.TotalJobs)
	}
	if report.Succeeded != 3 {
		t.Errorf("Succeeded = %d, want 3", report.Succeeded)
	}
	if report.Failed != 1 {
		t.Errorf("Failed = %d, want 1", report.Failed)
	}
	if report.ScalingEvents == nil {
		t.Error("ScalingEvents should default to empty slice, not nil")
	}

	wantAvg := (10.0 + 20.0 + 30.0) / 3
	if diff := report.AverageLatencySecs - wantAvg; diff > 0.01 || diff < -0.01 {
		t.Errorf("AverageLatencySecs = %v, want %v", report.AverageLatencySecs, wantAvg)
	}
	if report.P50LatencySecs != 20 {
		t.Errorf("P50LatencySecs = %v, want 20", report.P50LatencySecs)
	}
	if report.P99LatencySecs != 30 {
		t.Errorf("P99LatencySecs = %v, want 30", report.P99LatencySecs)
	}
}

func TestBuildReport_NoSuccesses(t *testing.T) {
	results := []jobResult{
		{SubmittedAt: time.Now(), CompletedAt: time.Now(), Succeeded: false},
	}
	report := BuildReport(results, nil)
	if report.AverageLatencySecs != 0 || report.P50LatencySecs != 0 {
		t.Errorf("expected zero latencies with no successes, got %+v", report)
	}
}

func TestRenderMarkdownSummary_Pass(t *testing.T) {
	cfg := DefaultConfig()
	report := Report{
		TotalJobs: 10, Succeeded: 10, Failed: 0,
		P50LatencySecs: 100, P99LatencySecs: 500,
		GeneratedAt: time.Now(),
	}
	summary := RenderMarkdownSummary(report, cfg)
	if !strings.Contains(summary, "Overall: PASS") {
		t.Errorf("expected PASS summary, got:\n%s", summary)
	}
}

func TestRenderMarkdownSummary_FailsOnLatency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.P99TargetSeconds = 100
	report := Report{
		TotalJobs: 10, Succeeded: 10, Failed: 0,
		P50LatencySecs: 50, P99LatencySecs: 5000,
		GeneratedAt: time.Now(),
	}
	summary := RenderMarkdownSummary(report, cfg)
	if !strings.Contains(summary, "Overall: FAIL") {
		t.Errorf("expected FAIL summary, got:\n%s", summary)
	}
	if !strings.Contains(summary, "FAIL | observed 5000") {
		t.Errorf("expected p99 line to show FAIL with observed value, got:\n%s", summary)
	}
}

func TestRenderMarkdownSummary_FailsOnSuccessRate(t *testing.T) {
	cfg := DefaultConfig()
	report := Report{
		TotalJobs: 10, Succeeded: 8, Failed: 2,
		P50LatencySecs: 1, P99LatencySecs: 1,
		GeneratedAt: time.Now(),
	}
	summary := RenderMarkdownSummary(report, cfg)
	if !strings.Contains(summary, "Overall: FAIL") {
		t.Errorf("expected FAIL summary for 80%% success rate below default 95%% target, got:\n%s", summary)
	}
}

func TestWriteJSONReport(t *testing.T) {
	report := BuildReport(nil, nil)
	b, err := WriteJSONReport(report)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(b), `"total_jobs": 0`) {
		t.Errorf("expected JSON to contain total_jobs field, got: %s", string(b))
	}
}
