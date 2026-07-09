package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	cfg, err := ParseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "load-test config error: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	report, summary, err := RunLoadTest(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load test failed: %v\n", err)
		os.Exit(1)
	}

	reportPath := filepath.Join(cfg.OutputDir, "report.json")
	summaryPath := filepath.Join(cfg.OutputDir, "summary.md")

	if err := writeJSONFile(reportPath, report); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(summaryPath, []byte(summary), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("load test complete\nreport: %s\nsummary: %s\n", reportPath, summaryPath)
}
