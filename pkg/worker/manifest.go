package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"pulsegrid/pkg"
)

// GenerateManifest builds a pkg.Manifest from a job's transcoding results,
// writes it as manifest.json inside destDir, and returns the manifest.
// singleResults and hlsResults are keyed by rendition ID; every rendition in
// job.Renditions must have a matching entry in the appropriate map.
func (t *Transcoder) GenerateManifest(ctx context.Context, job pkg.Job, singleResults map[string]RenditionResult, hlsResults map[string]HLSResult, destDir string) (pkg.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return pkg.Manifest{}, err
	}

	prefix := strings.TrimSuffix(job.OutputS3Prefix, "/")
	outputFiles := make([]pkg.OutputFile, 0, len(job.Renditions))

	for _, r := range job.Renditions {
		if r.HLS {
			res, ok := hlsResults[r.ID]
			if !ok {
				return pkg.Manifest{}, fmt.Errorf("generate manifest: missing hls result for rendition %s", r.ID)
			}
			outputFiles = append(outputFiles, pkg.OutputFile{
				Rendition: res.RenditionID,
				Path:      fmt.Sprintf("%s/%s/playlist.m3u8", prefix, r.ID),
			})
			continue
		}

		res, ok := singleResults[r.ID]
		if !ok {
			return pkg.Manifest{}, fmt.Errorf("generate manifest: missing result for rendition %s", r.ID)
		}
		outputFiles = append(outputFiles, pkg.OutputFile{
			Rendition:       res.RenditionID,
			Path:            fmt.Sprintf("%s/%s/%s", prefix, r.ID, filepath.Base(res.FilePath)),
			SizeBytes:       res.FileSizeBytes,
			DurationSeconds: res.DurationSeconds,
		})
	}

	manifest := pkg.Manifest{
		JobID:          job.ID,
		SourceFile:     job.SourceS3URI,
		OutputFiles:    outputFiles,
		GenerationTime: time.Now().UTC().Format(time.RFC3339),
		WorkerPodID:    os.Getenv("HOSTNAME"),
		FFmpegVersion:  t.ffmpegVersion(),
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return pkg.Manifest{}, fmt.Errorf("generate manifest: marshal json: %w", err)
	}

	if err := os.WriteFile(filepath.Join(destDir, "manifest.json"), data, 0o644); err != nil {
		return pkg.Manifest{}, fmt.Errorf("generate manifest: write file: %w", err)
	}

	return manifest, nil
}

// ffmpegVersion runs "ffmpeg -version" and extracts the version token from
// its first output line (e.g. "ffmpeg version 6.1.1 Copyright ..." → "6.1.1").
// Returns "unknown" if the binary can't be run or its output is unrecognized.
func (t *Transcoder) ffmpegVersion() string {
	out, err := exec.Command(t.ffmpegPath, "-version").Output()
	if err != nil {
		return "unknown"
	}
	firstLine := strings.SplitN(string(out), "\n", 2)[0]
	fields := strings.Fields(firstLine)
	if len(fields) >= 3 && fields[0] == "ffmpeg" && fields[1] == "version" {
		return fields[2]
	}
	return "unknown"
}
