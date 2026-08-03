package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Run submits cfg.NumJobs synthetic upload jobs spread evenly across
// cfg.BurstDuration, polls each to a terminal status, and returns the
// aggregated Report. Submission and polling both run concurrently (one
// goroutine per job) so the harness's own client-side latency doesn't
// distort the measured server-side latency.
func Run(ctx context.Context, cfg Config, httpClient *http.Client) (Report, error) {
	if cfg.NumJobs <= 0 {
		return Report{}, fmt.Errorf("NumJobs must be positive, got %d", cfg.NumJobs)
	}

	spacing := cfg.BurstDuration / time.Duration(cfg.NumJobs)

	results := make([]jobResult, cfg.NumJobs)
	var wg sync.WaitGroup

	for i := 0; i < cfg.NumJobs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			if spacing > 0 {
				time.Sleep(spacing * time.Duration(i))
			}

			sourceName := fmt.Sprintf("load-test-%d.mp4", i)
			jobID, submittedAt, err := submitJob(ctx, httpClient, cfg.APIBaseURL, sourceName, cfg.VideoSizeBytes, cfg.TargetRenditions)
			if err != nil {
				log.Printf("load-test job=%d event=submit_failed error=%v", i, err)
				results[i] = jobResult{SubmittedAt: time.Now(), CompletedAt: time.Now(), Succeeded: false, Err: err}
				return
			}

			results[i] = pollJob(ctx, httpClient, cfg.APIBaseURL, jobID, submittedAt, cfg.PollInterval, cfg.PollTimeout)
		}(i)
	}

	wg.Wait()

	// Best-effort: no Kubernetes client-go dependency is added purely to
	// observe replica counts (CLAUDE.md: no new dependencies without clear
	// value) — the free-tier node group tops out at 2 nodes, so pod-scaling
	// observation adds little over what KEDA's own metrics already show in
	// Grafana (see monitoring/grafana/dashboard.json's Pod Count panel).
	return BuildReport(results, nil), nil
}
