package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	cfg, err := ParseConfigFromEnv(osGetenv)
	if err != nil {
		log.Fatalf("load-test: invalid config: %v", err)
	}

	log.Printf("load-test: starting run against %s: %d jobs over %s, renditions=%v", cfg.APIBaseURL, cfg.NumJobs, cfg.BurstDuration, cfg.TargetRenditions)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.BurstDuration+cfg.PollTimeout+time.Minute)
	defer cancel()

	httpClient := &http.Client{Timeout: 2 * time.Minute}

	report, err := Run(ctx, cfg, httpClient)
	if err != nil {
		log.Fatalf("load-test: run failed: %v", err)
	}

	reportJSON, err := WriteJSONReport(report)
	if err != nil {
		log.Fatalf("load-test: marshal report: %v", err)
	}
	if err := os.WriteFile(cfg.ReportPath, reportJSON, 0o644); err != nil {
		log.Fatalf("load-test: write report: %v", err)
	}

	summary := RenderMarkdownSummary(report, cfg)
	if err := os.WriteFile(cfg.SummaryPath, []byte(summary), 0o644); err != nil {
		log.Fatalf("load-test: write summary: %v", err)
	}

	log.Printf("load-test: done. total=%d succeeded=%d failed=%d p50=%.1fs p99=%.1fs report=%s summary=%s",
		report.TotalJobs, report.Succeeded, report.Failed, report.P50LatencySecs, report.P99LatencySecs, cfg.ReportPath, cfg.SummaryPath)
}
