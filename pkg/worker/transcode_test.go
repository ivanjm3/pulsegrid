package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pulsegrid/pkg"
)

// writeFakeFFmpeg writes an executable shell script at dir/ffmpeg that runs
// body, standing in for the real ffmpeg binary in tests. body receives its
// arguments as "$@"; a common pattern is dumping them to a file for the test
// to inspect afterward.
func writeFakeFFmpeg(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "ffmpeg")
	script := "#!/bin/sh\n" + body + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

func testRendition() pkg.Rendition {
	return pkg.Rendition{ID: "720p", Codec: "libx264", BitrateKbps: 2500, Width: 1280, Height: 720}
}

func TestTranscodeSingleRendition_Success(t *testing.T) {
	dir := t.TempDir()
	// Last arg is the output path; write a fake output file and emit a
	// Duration line to stderr, mirroring real ffmpeg behavior.
	ffmpeg := writeFakeFFmpeg(t, dir, `
echo "Duration: 00:01:30.50, start: 0.000000, bitrate: 1000 kb/s" >&2
eval out=\${$#}
echo "fake mp4 bytes" > "$out"
exit 0
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	result, err := tr.TranscodeSingleRendition(context.Background(), "job-1", "/tmp/job-1/original.mp4", dir, testRendition())
	if err != nil {
		t.Fatalf("TranscodeSingleRendition returned error: %v", err)
	}
	if result.RenditionID != "720p" {
		t.Fatalf("RenditionID = %q, want 720p", result.RenditionID)
	}
	if result.FilePath != filepath.Join(dir, "720p.mp4") {
		t.Fatalf("FilePath = %q", result.FilePath)
	}
	if result.FileSizeBytes == 0 {
		t.Fatalf("FileSizeBytes = 0, want > 0")
	}
	if result.DurationSeconds != 90 {
		t.Fatalf("DurationSeconds = %d, want 90", result.DurationSeconds)
	}
}

func TestTranscodeSingleRendition_CommandBuiltCorrectly(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	ffmpeg := writeFakeFFmpeg(t, dir, `
echo "$@" > `+argsFile+`
eval out=\${$#}
touch "$out"
exit 0
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	source := "/tmp/job-2/original.mp4"
	if _, err := tr.TranscodeSingleRendition(context.Background(), "job-2", source, dir, testRendition()); err != nil {
		t.Fatalf("TranscodeSingleRendition returned error: %v", err)
	}

	got, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read captured args: %v", err)
	}
	args := string(got)
	for _, want := range []string{
		"-i " + source,
		"-c:v libx264",
		"-b:v 2500k",
		"-s 1280x720",
		"-c:a aac",
		"-b:a 128k",
		"-y",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("ffmpeg args %q missing %q", args, want)
		}
	}
}

func TestTranscodeSingleRendition_InvalidCodec_ExitNonZero(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := writeFakeFFmpeg(t, dir, `
echo "Unknown encoder 'bogus_codec'" >&2
exit 1
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	rendition := testRendition()
	rendition.Codec = "bogus_codec"
	_, err := tr.TranscodeSingleRendition(context.Background(), "job-3", "/tmp/job-3/original.mp4", dir, rendition)
	if err == nil {
		t.Fatal("expected error for invalid codec")
	}
	var tErr *pkg.TranscodingError
	if !errors.As(err, &tErr) {
		t.Fatalf("error = %v, want *pkg.TranscodingError", err)
	}
	if !strings.Contains(tErr.Stderr, "Unknown encoder") {
		t.Fatalf("Stderr = %q, want it to contain ffmpeg's error output", tErr.Stderr)
	}
}

func TestTranscodeSingleRendition_TimeoutExceeded_ProcessKilled(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := writeFakeFFmpeg(t, dir, `
sleep 5
exit 0
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg
	tr.timeout = 50 * time.Millisecond

	start := time.Now()
	_, err := tr.TranscodeSingleRendition(context.Background(), "job-4", "/tmp/job-4/original.mp4", dir, testRendition())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("TranscodeSingleRendition took %s, want it to return promptly after timeout", elapsed)
	}
	var tErr *pkg.TranscodingError
	if !errors.As(err, &tErr) {
		t.Fatalf("error = %v, want *pkg.TranscodingError", err)
	}
	if !strings.Contains(tErr.Err.Error(), "timed out") {
		t.Fatalf("underlying error = %v, want it to mention timeout", tErr.Err)
	}
}
