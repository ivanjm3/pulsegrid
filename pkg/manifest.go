package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ManifestOutputFile describes a single output file in the manifest.
type ManifestOutputFile struct {
	RenditionID     string  `json:"rendition_id"`
	FilePath        string  `json:"file_path"`
	FileSize        int64   `json:"file_size"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// Manifest is the JSON manifest written after transcoding completes.
type Manifest struct {
	JobID          string               `json:"job_id"`
	SourceFile     string               `json:"source_file"`
	OutputFiles    []ManifestOutputFile  `json:"output_files"`
	GenerationTime string               `json:"generation_time"`
	WorkerPodID    string               `json:"worker_pod_id"`
	FFmpegVersion  string               `json:"ffmpeg_version"`
}

// GenerateManifest builds manifest.json from transcode results and writes it to tempDir.
// Returns path to written manifest file.
func GenerateManifest(ctx context.Context, jobID string, sourceFile string, results []*TranscodeResult, hlsResults []*HLSResult, tempDir string) (string, error) {
	// Worker pod ID from HOSTNAME env (Kubernetes injects this)
	workerPodID := os.Getenv("HOSTNAME")
	if workerPodID == "" {
		workerPodID = "unknown"
	}

	// Get ffmpeg version
	ffmpegVersion := getFFmpegVersion()

	// Build output files from transcode results
	var outputFiles []ManifestOutputFile

	for _, r := range results {
		outputFiles = append(outputFiles, ManifestOutputFile{
			RenditionID:     r.RenditionID,
			FilePath:        r.FilePath,
			FileSize:        r.FileSize,
			DurationSeconds: r.DurationSeconds,
		})
	}

	for _, h := range hlsResults {
		// For HLS, include playlist as output file
		var totalSize int64
		for _, seg := range h.Segments {
			info, err := os.Stat(seg)
			if err == nil {
				totalSize += info.Size()
			}
		}
		// Also add playlist file size
		if info, err := os.Stat(h.PlaylistPath); err == nil {
			totalSize += info.Size()
		}

		outputFiles = append(outputFiles, ManifestOutputFile{
			RenditionID:     h.RenditionID,
			FilePath:        h.PlaylistPath,
			FileSize:        totalSize,
			DurationSeconds: 0, // HLS duration not tracked at playlist level
		})
	}

	manifest := Manifest{
		JobID:          jobID,
		SourceFile:     sourceFile,
		OutputFiles:    outputFiles,
		GenerationTime: time.Now().UTC().Format(time.RFC3339),
		WorkerPodID:    workerPodID,
		FFmpegVersion:  ffmpegVersion,
	}

	// Marshal to indented JSON
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("manifest marshal failed: %w", err)
	}

	// Validate JSON by unmarshalling back
	var check Manifest
	if err := json.Unmarshal(data, &check); err != nil {
		return "", fmt.Errorf("manifest validation failed: %w", err)
	}

	// Write to file
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", fmt.Errorf("cannot create manifest dir: %w", err)
	}

	if err := os.WriteFile(manifestPath, data, 0644); err != nil {
		return "", fmt.Errorf("manifest write failed: %w", err)
	}

	return manifestPath, nil
}

// getFFmpegVersion runs "ffmpeg -version" and extracts version string.
func getFFmpegVersion() string {
	cmd := exec.Command("ffmpeg", "-version")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}

	// First line: "ffmpeg version X.Y.Z ..."
	lines := strings.Split(string(output), "\n")
	if len(lines) > 0 {
		parts := strings.Fields(lines[0])
		if len(parts) >= 3 {
			return parts[2]
		}
		return lines[0]
	}
	return "unknown"
}
