package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pulsegrid/pkg"
)

func TestTranscodeHLS_CommandBuiltCorrectly(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	ffmpeg := writeFakeFFmpeg(t, dir, `
echo "$@" > `+argsFile+`
eval out=\${$#}
mkdir -p "$(dirname "$out")"
touch "$out"
touch "$(dirname "$out")/segment-00000.ts"
touch "$(dirname "$out")/segment-00001.ts"
exit 0
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	source := "/tmp/job-hls/original.mp4"
	if _, err := tr.TranscodeHLS(context.Background(), "job-hls", source, dir, testRendition()); err != nil {
		t.Fatalf("TranscodeHLS returned error: %v", err)
	}

	got, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read captured args: %v", err)
	}
	args := string(got)
	for _, want := range []string{
		"-i " + source,
		"-f hls",
		"-hls_time 6",
		"-hls_list_size 0",
		filepath.Join(dir, "hls", "playlist.m3u8"),
	} {
		if !strings.Contains(args, want) {
			t.Errorf("ffmpeg args %q missing %q", args, want)
		}
	}
}

func TestTranscodeHLS_PlaylistAndSegmentsCreated(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := writeFakeFFmpeg(t, dir, `
eval out=\${$#}
outdir=$(dirname "$out")
mkdir -p "$outdir"
echo "#EXTM3U" > "$out"
touch "$outdir/segment-00000.ts"
touch "$outdir/segment-00001.ts"
touch "$outdir/segment-00002.ts"
exit 0
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	result, err := tr.TranscodeHLS(context.Background(), "job-hls-2", "/tmp/job-hls-2/original.mp4", dir, testRendition())
	if err != nil {
		t.Fatalf("TranscodeHLS returned error: %v", err)
	}
	if result.PlaylistPath != filepath.Join(dir, "hls", "playlist.m3u8") {
		t.Fatalf("PlaylistPath = %q", result.PlaylistPath)
	}
	if _, err := os.Stat(result.PlaylistPath); err != nil {
		t.Fatalf("playlist.m3u8 not created: %v", err)
	}
	if result.SegmentCount != 3 {
		t.Fatalf("SegmentCount = %d, want 3", result.SegmentCount)
	}
}

func TestTranscodeHLS_FFmpegError_NoPlaylist(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := writeFakeFFmpeg(t, dir, `
echo "Invalid HLS options" >&2
exit 1
`)

	tr := NewTranscoder()
	tr.ffmpegPath = ffmpeg

	_, err := tr.TranscodeHLS(context.Background(), "job-hls-3", "/tmp/job-hls-3/original.mp4", dir, testRendition())
	if err == nil {
		t.Fatal("expected error when ffmpeg fails")
	}
	var tErr *pkg.TranscodingError
	if !errors.As(err, &tErr) {
		t.Fatalf("error = %v, want *pkg.TranscodingError", err)
	}
}
