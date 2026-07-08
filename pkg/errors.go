package pkg

import "fmt"

// TranscodingError represents a failure during the ffmpeg transcoding process.
type TranscodingError struct {
	JobID   string
	Message string
	Stderr  string
}

func (e *TranscodingError) Error() string {
	return fmt.Sprintf("transcoding error [job=%s]: %s", e.JobID, e.Message)
}

// ResourceConstraintError represents an unrecoverable resource issue (OOM, disk full).
type ResourceConstraintError struct {
	JobID      string
	Resource   string // "disk" or "memory"
	Message    string
}

func (e *ResourceConstraintError) Error() string {
	return fmt.Sprintf("resource constraint [job=%s, resource=%s]: %s", e.JobID, e.Resource, e.Message)
}
