package worker

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestCleanupTempDir_RemovesStagingDirectory is Property 9: Temporary File
// Cleanup. Validates Requirements 3.6: for any job, once processing finishes
// (success or failure), /tmp/{job_id} and everything under it no longer
// exists.
func TestCleanupTempDir_RemovesStagingDirectory(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(1))

	for i := 0; i < iterations; i++ {
		// success path: job "completed" with output files left behind
		jobID := fmt.Sprintf("cleanup-success-%d-%d", i, rnd.Int())
		mustCreateJobTempFiles(t, jobID, rnd)

		CleanupTempDir(jobID)

		if _, err := os.Stat(filepath.Join(os.TempDir(), jobID)); !os.IsNotExist(err) {
			t.Fatalf("job %s: temp dir still exists after cleanup (success path): err=%v", jobID, err)
		}
	}

	for i := 0; i < iterations; i++ {
		// failure path: same cleanup call is used regardless of how the job
		// ended, so it must remove partial/incomplete staging output too.
		jobID := fmt.Sprintf("cleanup-failure-%d-%d", i, rnd.Int())
		mustCreateJobTempFiles(t, jobID, rnd)

		CleanupTempDir(jobID)

		if _, err := os.Stat(filepath.Join(os.TempDir(), jobID)); !os.IsNotExist(err) {
			t.Fatalf("job %s: temp dir still exists after cleanup (failure path): err=%v", jobID, err)
		}
	}
}

func TestCleanupTempDir_MissingDirectoryIsNotAnError(t *testing.T) {
	// A job that failed before any staging happened (e.g. download never
	// created the dir) must not cause cleanup to panic or error out loudly.
	CleanupTempDir("job-that-never-staged-anything")
}

// mustCreateJobTempFiles creates a random number of nested files under
// /tmp/{jobID}, simulating a partially- or fully-staged job (source file,
// rendition outputs, manifest.json).
func mustCreateJobTempFiles(t *testing.T, jobID string, rnd *rand.Rand) {
	t.Helper()
	dir := filepath.Join(os.TempDir(), jobID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	n := rnd.Intn(5) + 1
	for i := 0; i < n; i++ {
		f := filepath.Join(dir, fmt.Sprintf("file-%d.bin", i))
		if err := os.WriteFile(f, []byte("data"), 0o644); err != nil {
			t.Fatalf("write temp file: %v", err)
		}
	}
	sub := filepath.Join(dir, "hls")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("create nested temp dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "playlist.m3u8"), []byte("#EXTM3U"), 0o644); err != nil {
		t.Fatalf("write nested temp file: %v", err)
	}
}
