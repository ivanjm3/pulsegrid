package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"pulsegrid/pkg"
)

// TestE2E_RealFFmpegTranscode runs an actual ffmpeg transcode (no mocks) against
// the sample.mp4 fixture at the repo root, verifying ffmpeg is on PATH and produces
// a valid output rendition.
func TestE2E_RealFFmpegTranscode(t *testing.T) {
	src, err := filepath.Abs(filepath.Join("..", "..", "sample.mp4"))
	if err != nil {
		t.Fatalf("resolve sample.mp4: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("sample.mp4 not found: %v", err)
	}

	workDir := t.TempDir()
	localSrc := filepath.Join(workDir, "sample.mp4")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read sample.mp4: %v", err)
	}
	if err := os.WriteFile(localSrc, data, 0644); err != nil {
		t.Fatalf("write local sample.mp4: %v", err)
	}

	rendition := pkg.Rendition{
		ID:           "360p",
		Resolution:   "640x360",
		VideoCodec:   "libx264",
		VideoBitrate: "500k",
		AudioCodec:   "aac",
		AudioBitrate: "96k",
	}

	result, err := pkg.TranscodeSingleRendition(context.Background(), localSrc, rendition, "e2e-test-job")
	if err != nil {
		t.Fatalf("TranscodeSingleRendition failed: %v", err)
	}

	if result.FileSize <= 0 {
		t.Fatalf("expected non-empty output file, got size=%d", result.FileSize)
	}
	if result.DurationSeconds <= 0 {
		t.Fatalf("expected parsed duration > 0, got %f", result.DurationSeconds)
	}
	if _, err := os.Stat(result.FilePath); err != nil {
		t.Fatalf("output file missing on disk: %v", err)
	}

	t.Logf("E2E transcode OK: rendition=%s size=%d bytes duration=%.2fs path=%s",
		result.RenditionID, result.FileSize, result.DurationSeconds, result.FilePath)
}
