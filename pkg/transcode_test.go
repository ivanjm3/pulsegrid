package pkg

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestTranscodeHLS_CreatesHLSDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	if err := os.WriteFile(sourceFile, []byte("fake video"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:              "hls-720p",
		Type:            "hls_segments",
		SegmentDuration: 6,
		BaseResolution:  "1280x720",
	}

	// ffmpeg will fail on fake input, but hls dir should be created
	_, _ = TranscodeHLS(context.Background(), sourceFile, rendition, "job-hls-1")

	hlsDir := filepath.Join(tmpDir, "hls")
	info, err := os.Stat(hlsDir)
	if err != nil {
		t.Fatalf("hls directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("hls path is not a directory")
	}
}

func TestTranscodeHLS_FfmpegFailure_ReturnsTranscodingError(t *testing.T) {
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	if err := os.WriteFile(sourceFile, []byte("not a video"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:              "hls-480p",
		Type:            "hls_segments",
		SegmentDuration: 4,
		BaseResolution:  "854x480",
	}

	_, err := TranscodeHLS(context.Background(), sourceFile, rendition, "job-hls-fail")
	if err == nil {
		t.Fatal("expected error from ffmpeg on invalid input")
	}

	te, ok := err.(*TranscodingError)
	if !ok {
		t.Fatalf("expected *TranscodingError, got %T: %v", err, err)
	}
	if te.JobID != "job-hls-fail" {
		t.Errorf("expected JobID=job-hls-fail, got %s", te.JobID)
	}
}

func TestTranscodeHLS_DefaultSegmentDuration(t *testing.T) {
	// SegmentDuration=0 should default to 6
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	if err := os.WriteFile(sourceFile, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:              "hls-default",
		Type:            "hls_segments",
		SegmentDuration: 0, // should default to 6
	}

	// ffmpeg will fail, but we verify no panic and error is TranscodingError
	_, err := TranscodeHLS(context.Background(), sourceFile, rendition, "job-default-seg")
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*TranscodingError); !ok {
		t.Fatalf("expected *TranscodingError, got %T", err)
	}
}

func TestTranscodeHLS_SterrTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	if err := os.WriteFile(sourceFile, []byte("garbage content for stderr"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:             "hls-trunc",
		Type:           "hls_segments",
		BaseResolution: "1920x1080",
	}

	_, err := TranscodeHLS(context.Background(), sourceFile, rendition, "job-trunc-hls")
	if err == nil {
		t.Fatal("expected error")
	}

	te, ok := err.(*TranscodingError)
	if !ok {
		t.Fatalf("expected *TranscodingError, got %T", err)
	}
	if len(te.Stderr) > 500 {
		t.Errorf("stderr length %d exceeds 500 char cap", len(te.Stderr))
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   float64
	}{
		{
			name:  "standard duration",
			input: "Duration: 01:23:45.67, start: 0.000000",
			want:  5025.67,
		},
		{
			name:  "zero duration",
			input: "Duration: 00:00:00.00, start: 0.000000",
			want:  0,
		},
		{
			name:  "seconds only",
			input: "  Duration: 00:00:30.50, start: 0.000000, bitrate: 5000 kb/s",
			want:  30.50,
		},
		{
			name:  "minutes and seconds",
			input: "Duration: 00:05:10.25",
			want:  310.25,
		},
		{
			name:  "no duration found",
			input: "some random ffmpeg output without duration",
			want:  0,
		},
		{
			name:  "multiline output with duration",
			input: "Input #0, mov,mp4\n  Duration: 02:00:00.00, start: 0.0\n  Stream #0:0",
			want:  7200.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDuration(tt.input)
			if got != tt.want {
				t.Errorf("parseDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTranscodeSingleRendition_FfmpegNotFound(t *testing.T) {
	// When ffmpeg binary doesn't exist or fails, should return TranscodingError
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	if err := os.WriteFile(sourceFile, []byte("fake video"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:           "720p",
		Resolution:   "1280x720",
		VideoCodec:   "libx264",
		VideoBitrate: "5M",
		AudioCodec:   "aac",
		AudioBitrate: "128k",
	}

	_, err := TranscodeSingleRendition(context.Background(), sourceFile, rendition, "test-job-1")
	if err == nil {
		t.Fatal("expected error when ffmpeg processes invalid input")
	}

	// Should be TranscodingError
	te, ok := err.(*TranscodingError)
	if !ok {
		t.Fatalf("expected *TranscodingError, got %T: %v", err, err)
	}
	if te.JobID != "test-job-1" {
		t.Errorf("expected JobID=test-job-1, got %s", te.JobID)
	}
}

func TestTranscodeSingleRendition_SterrTruncation(t *testing.T) {
	// TranscodingError.Stderr should be capped at 500 chars
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "source.mp4")
	// Write enough garbage to produce stderr > 500 chars from ffmpeg
	if err := os.WriteFile(sourceFile, []byte("not a video file at all"), 0644); err != nil {
		t.Fatal(err)
	}

	rendition := Rendition{
		ID:           "480p",
		Resolution:   "854x480",
		VideoCodec:   "libx264",
		VideoBitrate: "2.5M",
	}

	_, err := TranscodeSingleRendition(context.Background(), sourceFile, rendition, "job-trunc")
	if err == nil {
		t.Fatal("expected error")
	}

	te, ok := err.(*TranscodingError)
	if !ok {
		t.Fatalf("expected *TranscodingError, got %T", err)
	}
	if len(te.Stderr) > 500 {
		t.Errorf("stderr length %d exceeds 500 char limit", len(te.Stderr))
	}
}
