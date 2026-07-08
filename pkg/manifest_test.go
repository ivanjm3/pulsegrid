package pkg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateManifest_BasicFlow(t *testing.T) {
	// Setup temp dir
	tempDir := t.TempDir()

	// Set HOSTNAME env
	os.Setenv("HOSTNAME", "worker-pod-test-123")
	defer os.Unsetenv("HOSTNAME")

	results := []*TranscodeResult{
		{RenditionID: "720p", FilePath: "/tmp/job1/720p.mp4", FileSize: 5242880, DurationSeconds: 120.5},
		{RenditionID: "480p", FilePath: "/tmp/job1/480p.mp4", FileSize: 2621440, DurationSeconds: 120.5},
	}

	manifestPath, err := GenerateManifest(context.Background(), "job-abc-123", "s3://bucket/source.mp4", results, nil, tempDir)
	if err != nil {
		t.Fatalf("GenerateManifest failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest file not found: %v", err)
	}

	// Verify path
	expectedPath := filepath.Join(tempDir, "manifest.json")
	if manifestPath != expectedPath {
		t.Errorf("path mismatch: got %s, want %s", manifestPath, expectedPath)
	}

	// Read and parse
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("cannot read manifest: %v", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Verify fields
	if m.JobID != "job-abc-123" {
		t.Errorf("job_id: got %q, want %q", m.JobID, "job-abc-123")
	}
	if m.SourceFile != "s3://bucket/source.mp4" {
		t.Errorf("source_file: got %q, want %q", m.SourceFile, "s3://bucket/source.mp4")
	}
	if m.WorkerPodID != "worker-pod-test-123" {
		t.Errorf("worker_pod_id: got %q, want %q", m.WorkerPodID, "worker-pod-test-123")
	}
	if len(m.OutputFiles) != 2 {
		t.Fatalf("output_files count: got %d, want 2", len(m.OutputFiles))
	}
	if m.OutputFiles[0].RenditionID != "720p" {
		t.Errorf("output[0].rendition_id: got %q, want %q", m.OutputFiles[0].RenditionID, "720p")
	}
	if m.OutputFiles[0].FileSize != 5242880 {
		t.Errorf("output[0].file_size: got %d, want 5242880", m.OutputFiles[0].FileSize)
	}

	// Verify generation_time is valid ISO 8601
	_, err = time.Parse(time.RFC3339, m.GenerationTime)
	if err != nil {
		t.Errorf("generation_time not valid RFC3339: %q, err: %v", m.GenerationTime, err)
	}
}

func TestGenerateManifest_EmptyResults(t *testing.T) {
	tempDir := t.TempDir()

	manifestPath, err := GenerateManifest(context.Background(), "empty-job", "s3://b/f.mp4", nil, nil, tempDir)
	if err != nil {
		t.Fatalf("GenerateManifest failed: %v", err)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("cannot read: %v", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if m.OutputFiles != nil && len(m.OutputFiles) != 0 {
		t.Errorf("expected nil/empty output_files, got %d", len(m.OutputFiles))
	}
}

func TestGenerateManifest_WithHLSResults(t *testing.T) {
	tempDir := t.TempDir()

	// Create fake segment files so os.Stat works
	hlsDir := filepath.Join(tempDir, "hls")
	os.MkdirAll(hlsDir, 0755)

	seg1 := filepath.Join(hlsDir, "segment-00000.ts")
	seg2 := filepath.Join(hlsDir, "segment-00001.ts")
	playlist := filepath.Join(hlsDir, "playlist.m3u8")

	os.WriteFile(seg1, make([]byte, 1024), 0644)
	os.WriteFile(seg2, make([]byte, 2048), 0644)
	os.WriteFile(playlist, []byte("#EXTM3U\n"), 0644)

	hlsResults := []*HLSResult{
		{
			RenditionID:  "hls",
			PlaylistPath: playlist,
			SegmentCount: 2,
			Segments:     []string{seg1, seg2},
		},
	}

	results := []*TranscodeResult{
		{RenditionID: "720p", FilePath: "/tmp/720p.mp4", FileSize: 100000, DurationSeconds: 60.0},
	}

	manifestPath, err := GenerateManifest(context.Background(), "hls-job", "s3://b/vid.mp4", results, hlsResults, tempDir)
	if err != nil {
		t.Fatalf("GenerateManifest failed: %v", err)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("cannot read: %v", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Should have 2 output files: 720p + hls
	if len(m.OutputFiles) != 2 {
		t.Fatalf("output_files count: got %d, want 2", len(m.OutputFiles))
	}

	// HLS entry
	hlsEntry := m.OutputFiles[1]
	if hlsEntry.RenditionID != "hls" {
		t.Errorf("hls rendition_id: got %q", hlsEntry.RenditionID)
	}
	// Total size = seg1(1024) + seg2(2048) + playlist(8 bytes for "#EXTM3U\n")
	expectedSize := int64(1024 + 2048 + 8)
	if hlsEntry.FileSize != expectedSize {
		t.Errorf("hls file_size: got %d, want %d", hlsEntry.FileSize, expectedSize)
	}
}

func TestGenerateManifest_NoHostname(t *testing.T) {
	tempDir := t.TempDir()

	os.Unsetenv("HOSTNAME")

	manifestPath, err := GenerateManifest(context.Background(), "no-host-job", "s3://b/f.mp4", nil, nil, tempDir)
	if err != nil {
		t.Fatalf("GenerateManifest failed: %v", err)
	}

	data, _ := os.ReadFile(manifestPath)
	var m Manifest
	json.Unmarshal(data, &m)

	if m.WorkerPodID != "unknown" {
		t.Errorf("worker_pod_id should fallback to 'unknown', got %q", m.WorkerPodID)
	}
}
