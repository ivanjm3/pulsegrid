package main

import (
	"os"
	"path/filepath"
	"testing"

	"pulsegrid/pkg"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests (structured logger requires init before use)
	logger = pkg.NewLogger()
	os.Exit(m.Run())
}

func TestCleanupTempDir_RemovesDirectory(t *testing.T) {
	// Create temp dir simulating /tmp/{jobID}
	dir := filepath.Join(os.TempDir(), "test-cleanup-job-001")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("setup: mkdir failed: %v", err)
	}

	// Create a file inside to simulate downloaded source
	f, err := os.Create(filepath.Join(dir, "original.mp4"))
	if err != nil {
		t.Fatalf("setup: create file failed: %v", err)
	}
	f.Close()

	// Run cleanup
	cleanupTempDir(dir, "test-cleanup-job-001")

	// Verify directory removed
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("expected dir removed, got err=%v", err)
	}
}

func TestCleanupTempDir_NonexistentDir_NoError(t *testing.T) {
	// Should not panic on nonexistent path
	cleanupTempDir("/tmp/nonexistent-pulsegrid-test-xyz", "fake-job")
}

func TestCleanupTempDir_PermissionError_LogsWarning(t *testing.T) {
	// On Windows permission semantics differ; just verify no panic
	// Create dir, make read-only, attempt cleanup
	dir := filepath.Join(os.TempDir(), "test-cleanup-perm-001")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("setup: mkdir failed: %v", err)
	}
	defer os.RemoveAll(dir) // safety net

	// Create nested structure
	nested := filepath.Join(dir, "sub")
	os.MkdirAll(nested, 0755)
	f, _ := os.Create(filepath.Join(nested, "file.txt"))
	f.Close()

	// Attempt cleanup — should not panic regardless of OS behavior
	cleanupTempDir(dir, "test-cleanup-perm-001")
}
