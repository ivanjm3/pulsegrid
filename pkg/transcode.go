package pkg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// HLSResult holds metadata about a completed HLS transcode.
type HLSResult struct {
	RenditionID  string   `json:"rendition_id"`
	PlaylistPath string   `json:"playlist_path"`
	SegmentCount int      `json:"segment_count"`
	Segments     []string `json:"segments"`
}

// DefaultTranscodeTimeout is the maximum duration for a single ffmpeg invocation.
const DefaultTranscodeTimeout = 30 * time.Minute

// TranscodeResult holds metadata about a completed single-rendition transcode.
type TranscodeResult struct {
	RenditionID     string  `json:"rendition_id"`
	FilePath        string  `json:"file_path"`
	FileSize        int64   `json:"file_size"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// TranscodeSingleRendition invokes ffmpeg to transcode sourceFile into one rendition.
// Output is written to the same directory as sourceFile, named {rendition.ID}.mp4.
func TranscodeSingleRendition(ctx context.Context, sourceFile string, rendition Rendition, jobID string) (*TranscodeResult, error) {
	outputDir := filepath.Dir(sourceFile)
	outputFile := filepath.Join(outputDir, rendition.ID+".mp4")

	// Build ffmpeg args
	args := []string{"-i", sourceFile}

	if rendition.VideoCodec != "" {
		args = append(args, "-c:v", rendition.VideoCodec)
	}
	if rendition.VideoBitrate != "" {
		args = append(args, "-b:v", rendition.VideoBitrate)
	}
	if rendition.Resolution != "" {
		args = append(args, "-s", rendition.Resolution)
	}
	if rendition.AudioCodec != "" {
		args = append(args, "-c:a", rendition.AudioCodec)
	}
	if rendition.AudioBitrate != "" {
		args = append(args, "-b:a", rendition.AudioBitrate)
	}

	args = append(args, "-y", outputFile)

	// Create timeout context
	transcodeCtx, cancel := context.WithTimeout(ctx, DefaultTranscodeTimeout)
	defer cancel()

	cmd := exec.CommandContext(transcodeCtx, "ffmpeg", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		stderr := string(output)
		if len(stderr) > 500 {
			stderr = stderr[:500]
		}
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("ffmpeg exited with error: %v", err),
			Stderr:  stderr,
		}
	}

	// Parse duration from ffmpeg output
	duration := parseDuration(string(output))

	// Get output file size
	info, err := os.Stat(outputFile)
	if err != nil {
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("cannot stat output file: %v", err),
			Stderr:  "",
		}
	}

	return &TranscodeResult{
		RenditionID:     rendition.ID,
		FilePath:        outputFile,
		FileSize:        info.Size(),
		DurationSeconds: duration,
	}, nil
}

// TranscodeHLS invokes ffmpeg to produce HLS segments from sourceFile.
// Creates an "hls" subdirectory in sourceFile's parent, outputs playlist.m3u8 + segment-XXXXX.ts files.
func TranscodeHLS(ctx context.Context, sourceFile string, rendition Rendition, jobID string) (*HLSResult, error) {
	parentDir := filepath.Dir(sourceFile)
	hlsDir := filepath.Join(parentDir, "hls")

	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("cannot create hls directory: %v", err),
		}
	}

	playlistPath := filepath.Join(hlsDir, "playlist.m3u8")
	segmentPattern := filepath.Join(hlsDir, "segment-%05d.ts")

	// Default segment duration: 6 seconds
	segDuration := rendition.SegmentDuration
	if segDuration == 0 {
		segDuration = 6
	}

	// Build ffmpeg args
	args := []string{"-i", sourceFile}

	if rendition.BaseResolution != "" {
		args = append(args, "-s", rendition.BaseResolution)
	}

	args = append(args,
		"-f", "hls",
		"-hls_time", strconv.Itoa(segDuration),
		"-hls_segment_filename", segmentPattern,
		"-hls_list_size", "0",
		"-y",
		playlistPath,
	)

	// Timeout context
	transcodeCtx, cancel := context.WithTimeout(ctx, DefaultTranscodeTimeout)
	defer cancel()

	cmd := exec.CommandContext(transcodeCtx, "ffmpeg", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		stderr := string(output)
		if len(stderr) > 500 {
			stderr = stderr[:500]
		}
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("ffmpeg HLS exited with error: %v", err),
			Stderr:  stderr,
		}
	}

	// Verify playlist exists
	if _, err := os.Stat(playlistPath); err != nil {
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("playlist not found after ffmpeg: %v", err),
		}
	}

	// Glob for .ts segments
	segments, err := filepath.Glob(filepath.Join(hlsDir, "*.ts"))
	if err != nil {
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: fmt.Sprintf("cannot glob segments: %v", err),
		}
	}

	if len(segments) == 0 {
		return nil, &TranscodingError{
			JobID:   jobID,
			Message: "no segments found after successful ffmpeg run",
		}
	}

	return &HLSResult{
		RenditionID:  rendition.ID,
		PlaylistPath: playlistPath,
		SegmentCount: len(segments),
		Segments:     segments,
	}, nil
}

// durationRegex matches ffmpeg's "Duration: HH:MM:SS.xx" output line.
var durationRegex = regexp.MustCompile(`Duration: (\d{2}):(\d{2}):(\d{2}\.\d+)`)

// parseDuration extracts duration in seconds from ffmpeg combined output.
// Returns 0 if duration not found.
func parseDuration(output string) float64 {
	matches := durationRegex.FindStringSubmatch(output)
	if len(matches) < 4 {
		return 0
	}

	hours, _ := strconv.ParseFloat(matches[1], 64)
	minutes, _ := strconv.ParseFloat(matches[2], 64)
	seconds, _ := strconv.ParseFloat(matches[3], 64)

	return hours*3600 + minutes*60 + seconds
}
