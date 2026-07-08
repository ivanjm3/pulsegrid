package pkg

import (
	"encoding/json"
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

// generateRandomRenditions creates 0-5 renditions with varied codecs/bitrates.
func generateRandomRenditions(rng *rand.Rand) []Rendition {
	codecs := []string{"h264", "h265", "vp9", "av1"}
	resolutions := []string{"1920x1080", "1280x720", "854x480", "640x360", "3840x2160"}
	audioCodcs := []string{"aac", "opus", "mp3", "flac"}
	types := []string{"mp4", "hls_segments", "webm"}

	count := rng.Intn(6) // 0-5
	renditions := make([]Rendition, count)
	for i := range renditions {
		renditions[i] = Rendition{
			ID:             randomString(rng, 8),
			Resolution:     resolutions[rng.Intn(len(resolutions))],
			VideoCodec:     codecs[rng.Intn(len(codecs))],
			VideoBitrate:   randomBitrate(rng),
			AudioCodec:     audioCodcs[rng.Intn(len(audioCodcs))],
			AudioBitrate:   randomAudioBitrate(rng),
			Type:           types[rng.Intn(len(types))],
			SegmentDuration: rng.Intn(10) + 2,
			BaseResolution: resolutions[rng.Intn(len(resolutions))],
		}
	}
	return renditions
}

func randomString(rng *rand.Rand, n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func randomBitrate(rng *rand.Rand) string {
	rates := []string{"500k", "1000k", "2000k", "4000k", "8000k", "12000k"}
	return rates[rng.Intn(len(rates))]
}

func randomAudioBitrate(rng *rand.Rand) string {
	rates := []string{"64k", "128k", "192k", "256k", "320k"}
	return rates[rng.Intn(len(rates))]
}

// generateRandomJob creates a random Job with valid required fields.
func generateRandomJob(rng *rand.Rand) Job {
	return Job{
		JobID:                 randomString(rng, 32),
		SourceS3URI:           "s3://pulsegrid-source/" + randomString(rng, 16) + "/original.mp4",
		SourceFileName:        randomString(rng, 12) + ".mp4",
		SourceFileSizeBytes:   int64(rng.Intn(10_000_000_000)) + 1,
		Renditions:            generateRandomRenditions(rng),
		OutputS3Prefix:        "s3://pulsegrid-output/" + randomString(rng, 16),
		RetryCount:            rng.Intn(4),
		MaxRetries:            rng.Intn(5) + 1,
		Status:                JobStatusSubmitted,
		SubmissionTime:        time.Now().UTC().Add(-time.Duration(rng.Intn(3600)) * time.Second),
		VisibilityTimeoutSecs: rng.Intn(1800) + 30,
	}
}

// TestKafkaMessageSchema_PropertySchemaCompliance verifies that Job → KafkaMessage → JSON
// round-trip preserves all required fields with correct types.
//
// **Validates: Requirements 2.1, 1.6**
func TestKafkaMessageSchema_PropertySchemaCompliance(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		job := generateRandomJob(rng)

		// Job → KafkaMessage (same logic as EnqueueJob)
		msg := KafkaMessage{
			JobID:                 job.JobID,
			SourceS3URI:           job.SourceS3URI,
			SourceFileSizeBytes:   job.SourceFileSizeBytes,
			Renditions:            job.Renditions,
			OutputS3Prefix:        job.OutputS3Prefix,
			RetryCount:            job.RetryCount,
			MaxRetries:            job.MaxRetries,
			SubmittedTimestamp:    job.SubmissionTime.UTC().Format(time.RFC3339Nano),
			VisibilityTimeoutSecs: job.VisibilityTimeoutSecs,
		}

		// Marshal to JSON
		data, err := json.Marshal(msg)
		if err != nil {
			t.Logf("marshal failed: %v", err)
			return false
		}

		// Unmarshal to raw map — verify field presence and types
		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Logf("unmarshal to map failed: %v", err)
			return false
		}

		// Required field: job_id (non-empty string)
		jobID, ok := raw["job_id"].(string)
		if !ok || jobID == "" {
			t.Logf("job_id missing or empty")
			return false
		}

		// Required field: source_s3_uri (non-empty string)
		sourceURI, ok := raw["source_s3_uri"].(string)
		if !ok || sourceURI == "" {
			t.Logf("source_s3_uri missing or empty")
			return false
		}

		// Required field: source_file_size_bytes (> 0)
		sizeFloat, ok := raw["source_file_size_bytes"].(float64)
		if !ok || sizeFloat <= 0 {
			t.Logf("source_file_size_bytes missing or <= 0: %v", raw["source_file_size_bytes"])
			return false
		}

		// Required field: renditions (array)
		renditions, ok := raw["renditions"]
		if !ok {
			t.Logf("renditions field missing")
			return false
		}
		rendArr, ok := renditions.([]interface{})
		if !ok {
			t.Logf("renditions not array type")
			return false
		}

		// Rendition count matches input
		if len(rendArr) != len(job.Renditions) {
			t.Logf("rendition count mismatch: got %d, want %d", len(rendArr), len(job.Renditions))
			return false
		}

		// Required field: output_s3_prefix (non-empty string)
		prefix, ok := raw["output_s3_prefix"].(string)
		if !ok || prefix == "" {
			t.Logf("output_s3_prefix missing or empty")
			return false
		}

		// Required field: retry_count (>= 0)
		retryFloat, ok := raw["retry_count"].(float64)
		if !ok || retryFloat < 0 {
			t.Logf("retry_count missing or < 0")
			return false
		}

		// Required field: max_retries (> 0)
		maxRetriesFloat, ok := raw["max_retries"].(float64)
		if !ok || maxRetriesFloat <= 0 {
			t.Logf("max_retries missing or <= 0")
			return false
		}

		// Required field: submitted_timestamp (valid RFC3339)
		tsStr, ok := raw["submitted_timestamp"].(string)
		if !ok || tsStr == "" {
			t.Logf("submitted_timestamp missing or empty")
			return false
		}
		if _, err := time.Parse(time.RFC3339Nano, tsStr); err != nil {
			t.Logf("submitted_timestamp not valid RFC3339: %v", err)
			return false
		}

		// Required field: visibility_timeout_seconds (> 0)
		visFloat, ok := raw["visibility_timeout_seconds"].(float64)
		if !ok || visFloat <= 0 {
			t.Logf("visibility_timeout_seconds missing or <= 0")
			return false
		}

		// Verify round-trip: unmarshal back to KafkaMessage struct
		var roundTrip KafkaMessage
		if err := json.Unmarshal(data, &roundTrip); err != nil {
			t.Logf("round-trip unmarshal failed: %v", err)
			return false
		}

		// Verify values preserved after round-trip
		if roundTrip.JobID != msg.JobID {
			t.Logf("job_id mismatch after round-trip")
			return false
		}
		if roundTrip.SourceS3URI != msg.SourceS3URI {
			t.Logf("source_s3_uri mismatch after round-trip")
			return false
		}
		if roundTrip.SourceFileSizeBytes != msg.SourceFileSizeBytes {
			t.Logf("source_file_size_bytes mismatch after round-trip")
			return false
		}
		if roundTrip.OutputS3Prefix != msg.OutputS3Prefix {
			t.Logf("output_s3_prefix mismatch after round-trip")
			return false
		}
		if roundTrip.RetryCount != msg.RetryCount {
			t.Logf("retry_count mismatch after round-trip")
			return false
		}
		if roundTrip.MaxRetries != msg.MaxRetries {
			t.Logf("max_retries mismatch after round-trip")
			return false
		}
		if roundTrip.SubmittedTimestamp != msg.SubmittedTimestamp {
			t.Logf("submitted_timestamp mismatch after round-trip")
			return false
		}
		if roundTrip.VisibilityTimeoutSecs != msg.VisibilityTimeoutSecs {
			t.Logf("visibility_timeout_seconds mismatch after round-trip")
			return false
		}
		if len(roundTrip.Renditions) != len(msg.Renditions) {
			t.Logf("renditions length mismatch after round-trip")
			return false
		}

		return true
	}

	cfg := &quick.Config{MaxCount: 150}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}
