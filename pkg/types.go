package pkg

import (
	"crypto/rand"
	"fmt"
	"time"
)

// JobStatus represents the lifecycle state of a transcoding job.
type JobStatus string

const (
	JobStatusSubmitting JobStatus = "submitting"
	JobStatusSubmitted  JobStatus = "submitted"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// Rendition describes a single target output encoding.
type Rendition struct {
	ID          string `json:"id"`
	Codec       string `json:"codec"`
	BitrateKbps int    `json:"bitrate_kbps"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	HLS         bool   `json:"hls"`
}

// RetryConfig controls retry/backoff behavior for a job.
type RetryConfig struct {
	MaxAttempts int           `json:"max_attempts"`
	BaseDelay   time.Duration `json:"base_delay"`
	MaxDelay    time.Duration `json:"max_delay"`
}

// Job is the core unit of work submitted by a client.
type Job struct {
	ID                  string      `json:"job_id"`
	SourceName          string      `json:"source_name"`
	SourceFileSizeBytes int64       `json:"source_file_size_bytes"`
	SourceS3URI         string      `json:"source_s3_uri"`
	OutputS3Prefix      string      `json:"output_s3_prefix"`
	Renditions          []Rendition `json:"renditions"`
	Status              JobStatus   `json:"status"`
	RetryCount          int         `json:"retry_count"`
	SubmissionTime      time.Time   `json:"submission_time"`
	CompletionTime      *time.Time  `json:"completion_time,omitempty"`
}

// NewJobID generates an RFC 4122 version 4 UUID using crypto/rand.
func NewJobID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	// Set version (4) and variant (RFC 4122) bits.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
