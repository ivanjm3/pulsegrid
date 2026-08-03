package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"testing/quick"
	"time"

	"pulsegrid/pkg"
)

// buildRandomJobAndResults generates a job with 0-5 renditions (a mix of
// single-file and HLS) plus matching result maps, for Property 6.
func buildRandomJobAndResults(rnd *rand.Rand) (pkg.Job, map[string]RenditionResult, map[string]HLSResult) {
	n := rnd.Intn(6) // 0-5 renditions
	job := pkg.Job{
		ID:             fmt.Sprintf("job-%d", rnd.Intn(1_000_000)),
		SourceS3URI:    "s3://pulsegrid-source/job/original.mp4",
		OutputS3Prefix: "s3://pulsegrid-output/job",
	}
	single := map[string]RenditionResult{}
	hls := map[string]HLSResult{}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("rendition-%d", i)
		isHLS := rnd.Intn(2) == 0
		job.Renditions = append(job.Renditions, pkg.Rendition{ID: id, HLS: isHLS})
		if isHLS {
			hls[id] = HLSResult{RenditionID: id, PlaylistPath: "/tmp/job/hls/playlist.m3u8", SegmentCount: rnd.Intn(50) + 1}
		} else {
			single[id] = RenditionResult{
				RenditionID:     id,
				FilePath:        "/tmp/job/" + id + ".mp4",
				FileSizeBytes:   int64(rnd.Intn(1_000_000_000)),
				DurationSeconds: rnd.Intn(3600),
			}
		}
	}

	return job, single, hls
}

// TestGenerateManifest_Schema is Property 6: Manifest Generation Schema.
// Validates Requirements 4.4: for any completed transcoding with any number
// of output renditions, the generated manifest.json is valid JSON containing
// job_id, source_file, output_files, generation_time, worker_pod_id, and
// ffmpeg_version, with generation_time a valid ISO 8601 timestamp.
func TestGenerateManifest_Schema(t *testing.T) {
	const iterations = 150
	rnd := rand.New(rand.NewSource(1))
	tr := NewTranscoder()

	f := func(seed uint32) bool {
		src := rand.New(rand.NewSource(int64(seed) + int64(rnd.Int63())))
		job, single, hls := buildRandomJobAndResults(src)

		destDir := t.TempDir()
		manifest, err := tr.GenerateManifest(context.Background(), job, single, hls, destDir)
		if err != nil {
			t.Errorf("GenerateManifest: %v", err)
			return false
		}

		// Verify the written file is valid JSON with all required fields.
		raw, err := os.ReadFile(filepath.Join(destDir, "manifest.json"))
		if err != nil {
			t.Errorf("read manifest.json: %v", err)
			return false
		}
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Errorf("manifest.json is not valid JSON: %v", err)
			return false
		}

		for _, field := range []string{"job_id", "source_file", "output_files", "generation_time", "worker_pod_id", "ffmpeg_version"} {
			if _, ok := parsed[field]; !ok {
				t.Errorf("manifest missing required field %q", field)
				return false
			}
		}

		if manifest.JobID != job.ID {
			t.Errorf("JobID = %q, want %q", manifest.JobID, job.ID)
			return false
		}
		if len(manifest.OutputFiles) != len(job.Renditions) {
			t.Errorf("OutputFiles has %d entries, want %d", len(manifest.OutputFiles), len(job.Renditions))
			return false
		}
		if _, err := time.Parse(time.RFC3339, manifest.GenerationTime); err != nil {
			t.Errorf("GenerationTime %q is not valid ISO 8601: %v", manifest.GenerationTime, err)
			return false
		}
		if manifest.FFmpegVersion == "" {
			t.Errorf("FFmpegVersion is empty")
			return false
		}

		return true
	}

	cfg := &quick.Config{MaxCount: iterations}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}

func TestGenerateManifest_ZeroRenditions(t *testing.T) {
	tr := NewTranscoder()
	job := pkg.Job{ID: "job-empty", SourceS3URI: "s3://pulsegrid-source/job-empty/original.mp4", OutputS3Prefix: "s3://pulsegrid-output/job-empty"}
	destDir := t.TempDir()

	manifest, err := tr.GenerateManifest(context.Background(), job, nil, nil, destDir)
	if err != nil {
		t.Fatalf("GenerateManifest: %v", err)
	}
	if len(manifest.OutputFiles) != 0 {
		t.Fatalf("OutputFiles = %v, want empty", manifest.OutputFiles)
	}
}

func TestGenerateManifest_MissingResult_ReturnsError(t *testing.T) {
	tr := NewTranscoder()
	job := pkg.Job{
		ID:             "job-missing",
		OutputS3Prefix: "s3://pulsegrid-output/job-missing",
		Renditions:     []pkg.Rendition{{ID: "720p"}},
	}
	if _, err := tr.GenerateManifest(context.Background(), job, nil, nil, t.TempDir()); err == nil {
		t.Fatal("expected error for missing result")
	}
}
