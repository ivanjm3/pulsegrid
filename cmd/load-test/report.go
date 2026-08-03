package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// ScalingEvent records an observed worker replica count change. Populated
// only when a Kubernetes API is reachable (see run.go); left empty
// otherwise — the free-tier default node group (max_nodes=2 in
// terraform/variables.tf) never exercises the 0-to-50-pod scenario in
// requirements.md #16.1, so this harness does not depend on a cluster
// autoscaling API to produce a useful report.
type ScalingEvent struct {
	Timestamp    time.Time `json:"timestamp"`
	ReplicaCount int       `json:"replica_count"`
}

// Report is the JSON output of a load test run (requirements.md #10.4).
type Report struct {
	TotalJobs          int            `json:"total_jobs"`
	Succeeded          int            `json:"succeeded"`
	Failed             int            `json:"failed"`
	AverageLatencySecs float64        `json:"average_latency_seconds"`
	P50LatencySecs     float64        `json:"p50_latency_seconds"`
	P95LatencySecs     float64        `json:"p95_latency_seconds"`
	P99LatencySecs     float64        `json:"p99_latency_seconds"`
	ScalingEvents      []ScalingEvent `json:"scaling_events"`
	GeneratedAt        time.Time      `json:"generated_at"`
}

// sloResult is one pass/fail line in the markdown summary.
type sloResult struct {
	Name   string
	Pass   bool
	Detail string
}

// BuildReport aggregates per-job results into a Report. Latency is measured
// client-side (submit time to terminal-status-observed time), so it
// includes one poll interval of slack over true server-side completion
// time — acceptable for SLO validation, which targets minutes not seconds.
func BuildReport(results []jobResult, scalingEvents []ScalingEvent) Report {
	report := Report{
		TotalJobs:     len(results),
		ScalingEvents: scalingEvents,
		GeneratedAt:   time.Now().UTC(),
	}
	if scalingEvents == nil {
		report.ScalingEvents = []ScalingEvent{}
	}

	latencies := make([]float64, 0, len(results))
	for _, r := range results {
		if r.Succeeded {
			report.Succeeded++
			latencies = append(latencies, r.CompletedAt.Sub(r.SubmittedAt).Seconds())
		} else {
			report.Failed++
		}
	}

	if len(latencies) == 0 {
		return report
	}

	sort.Float64s(latencies)
	sum := 0.0
	for _, l := range latencies {
		sum += l
	}
	report.AverageLatencySecs = sum / float64(len(latencies))
	report.P50LatencySecs = percentile(latencies, 0.50)
	report.P95LatencySecs = percentile(latencies, 0.95)
	report.P99LatencySecs = percentile(latencies, 0.99)
	return report
}

// percentile computes the nearest-rank percentile over a pre-sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// WriteJSONReport marshals the report as indented JSON.
func WriteJSONReport(report Report) ([]byte, error) {
	return json.MarshalIndent(report, "", "  ")
}

// RenderMarkdownSummary produces a pass/fail markdown summary against the
// configured SLOs (requirements.md #10.6): success rate, p50 latency, p99
// latency.
func RenderMarkdownSummary(report Report, cfg Config) string {
	successRatePct := 0.0
	if report.TotalJobs > 0 {
		successRatePct = 100 * float64(report.Succeeded) / float64(report.TotalJobs)
	}

	results := []sloResult{
		{
			Name:   fmt.Sprintf("Success rate >= %.1f%%", cfg.MinSuccessRatePct),
			Pass:   successRatePct >= cfg.MinSuccessRatePct,
			Detail: fmt.Sprintf("observed %.1f%% (%d/%d)", successRatePct, report.Succeeded, report.TotalJobs),
		},
		{
			Name:   fmt.Sprintf("p50 latency <= %.0fs", cfg.P50TargetSeconds),
			Pass:   report.P50LatencySecs <= cfg.P50TargetSeconds,
			Detail: fmt.Sprintf("observed %.1fs", report.P50LatencySecs),
		},
		{
			Name:   fmt.Sprintf("p99 latency <= %.0fs", cfg.P99TargetSeconds),
			Pass:   report.P99LatencySecs <= cfg.P99TargetSeconds,
			Detail: fmt.Sprintf("observed %.1fs", report.P99LatencySecs),
		},
	}

	overallPass := true
	var b strings.Builder
	b.WriteString("# Pulsegrid Load Test Summary\n\n")
	fmt.Fprintf(&b, "Generated: %s\n\n", report.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Total jobs: %d | Succeeded: %d | Failed: %d\n\n", report.TotalJobs, report.Succeeded, report.Failed)
	b.WriteString("| SLO | Result | Detail |\n")
	b.WriteString("|---|---|---|\n")
	for _, r := range results {
		mark := "PASS"
		if !r.Pass {
			mark = "FAIL"
			overallPass = false
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", r.Name, mark, r.Detail)
	}
	b.WriteString("\n")
	if overallPass {
		b.WriteString("**Overall: PASS**\n")
	} else {
		b.WriteString("**Overall: FAIL**\n")
	}
	if len(report.ScalingEvents) == 0 {
		b.WriteString("\n_No scaling events recorded — this run did not have a reachable Kubernetes API, or the free-tier node group (max 2 nodes) never needed to scale._\n")
	}
	return b.String()
}
