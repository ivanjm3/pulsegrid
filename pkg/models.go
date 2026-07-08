package pkg

import "time"

// JobStatus represents the lifecycle state of a transcoding job.
type JobStatus string

const (
	JobStatusSubmitting JobStatus = "submitting"
	JobStatusSubmitted  JobStatus = "submitted"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// RetryConfig holds retry parameters for job processing.
type RetryConfig struct {
	MaxRetries int           `json:"max_retries"`
	BaseDelay  time.Duration `json:"base_delay"`
	MaxDelay   time.Duration `json:"max_delay"`
}

// DefaultRetryConfig returns standard retry settings (max 3, 1s base, 16s cap).
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		BaseDelay:  1 * time.Second,
		MaxDelay:   16 * time.Second,
	}
}

// Rendition defines a target output format for transcoding.
type Rendition struct {
	ID              string `json:"id"`
	Resolution      string `json:"resolution,omitempty"`
	VideoCodec      string `json:"video_codec,omitempty"`
	VideoBitrate    string `json:"video_bitrate,omitempty"`
	AudioCodec      string `json:"audio_codec,omitempty"`
	AudioBitrate    string `json:"audio_bitrate,omitempty"`
	Type            string `json:"type,omitempty"`             // e.g. "hls_segments"
	SegmentDuration int    `json:"segment_duration,omitempty"` // seconds, for HLS
	BaseResolution  string `json:"base_resolution,omitempty"` // for HLS
}

// Job represents a transcoding job in the system.
type Job struct {
	JobID                  string      `json:"job_id"`
	SourceS3URI            string      `json:"source_s3_uri"`
	SourceFileName         string      `json:"source_file_name"`
	SourceFileSizeBytes    int64       `json:"source_file_size_bytes"`
	Renditions             []Rendition `json:"renditions"`
	OutputS3Prefix         string      `json:"output_s3_prefix"`
	RetryCount             int         `json:"retry_count"`
	MaxRetries             int         `json:"max_retries"`
	Status                 JobStatus   `json:"status"`
	SubmissionTime         time.Time   `json:"submission_time"`
	ProcessingStartTime    *time.Time  `json:"processing_start_time,omitempty"`
	CompletionTime         *time.Time  `json:"completion_time,omitempty"`
	FailureReason          string      `json:"failure_reason,omitempty"`
	VisibilityTimeoutSecs  int         `json:"visibility_timeout_seconds"`
}
