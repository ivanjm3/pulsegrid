package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"pulsegrid/pkg"
)

// hlsSegmentSeconds is the target segment duration passed to ffmpeg's
// -hls_time flag, matching the design doc.
const hlsSegmentSeconds = 6

// HLSResult is the metadata produced by a successful HLS rendition
// transcode, used to build the job manifest (task 16).
type HLSResult struct {
	RenditionID  string
	PlaylistPath string
	SegmentCount int
}

// TranscodeHLS runs ffmpeg against sourceFile to produce an HLS rendition
// (playlist.m3u8 plus .ts segments) inside a "hls" subdirectory of destDir.
// On a non-zero ffmpeg exit code it returns a *pkg.TranscodingError carrying
// the captured stderr.
func (t *Transcoder) TranscodeHLS(ctx context.Context, jobID, sourceFile, destDir string, rendition pkg.Rendition) (HLSResult, error) {
	hlsDir := filepath.Join(destDir, "hls")
	if err := os.MkdirAll(hlsDir, 0o755); err != nil {
		return HLSResult{}, fmt.Errorf("transcode hls %s: create output dir: %w", rendition.ID, err)
	}
	playlistPath := filepath.Join(hlsDir, "playlist.m3u8")

	args := []string{
		"-i", sourceFile,
		"-c:v", rendition.Codec,
		"-b:v", fmt.Sprintf("%dk", rendition.BitrateKbps),
		"-s", fmt.Sprintf("%dx%d", rendition.Width, rendition.Height),
		"-c:a", "aac",
		"-b:a", "128k",
		"-f", "hls",
		"-hls_time", fmt.Sprintf("%d", hlsSegmentSeconds),
		"-hls_list_size", "0",
		playlistPath,
	}

	if _, err := t.run(ctx, jobID, rendition.ID, args); err != nil {
		return HLSResult{}, err
	}

	if _, err := os.Stat(playlistPath); err != nil {
		return HLSResult{}, fmt.Errorf("transcode hls %s: playlist not generated: %w", rendition.ID, err)
	}

	segments, err := filepath.Glob(filepath.Join(hlsDir, "*.ts"))
	if err != nil {
		return HLSResult{}, fmt.Errorf("transcode hls %s: list segments: %w", rendition.ID, err)
	}
	if len(segments) == 0 {
		return HLSResult{}, fmt.Errorf("transcode hls %s: no segments generated", rendition.ID)
	}

	return HLSResult{
		RenditionID:  rendition.ID,
		PlaylistPath: playlistPath,
		SegmentCount: len(segments),
	}, nil
}
