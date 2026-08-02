package pkg

import "fmt"

// TranscodingError indicates ffmpeg or codec-level failure while processing a rendition.
type TranscodingError struct {
	JobID     string
	Rendition string
	Stderr    string
	Err       error
}

func (e *TranscodingError) Error() string {
	return fmt.Sprintf("transcoding error job=%s rendition=%s: %v", e.JobID, e.Rendition, e.Err)
}

func (e *TranscodingError) Unwrap() error {
	return e.Err
}

// ResourceConstraintError indicates a pod-fatal condition (out of disk, OOM) that
// requires the pod to exit rather than retry.
type ResourceConstraintError struct {
	Resource string
	Err      error
}

func (e *ResourceConstraintError) Error() string {
	return fmt.Sprintf("resource constraint (%s): %v", e.Resource, e.Err)
}

func (e *ResourceConstraintError) Unwrap() error {
	return e.Err
}
