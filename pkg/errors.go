package pkg

import (
	"errors"
	"fmt"
	"strings"
)

// ErrJobNotFound is returned when a job_id does not exist in the database.
var ErrJobNotFound = errors.New("job not found")

// ErrorType classifies errors for retry/DLQ decision.
type ErrorType string

const (
	// ErrorTypeRetryable — transient failures that may succeed on retry.
	ErrorTypeRetryable ErrorType = "retryable"
	// ErrorTypePermanent — non-retryable failures, send to DLQ immediately.
	ErrorTypePermanent ErrorType = "permanent"
	// ErrorTypeConstraint — resource constraint, pod must exit immediately.
	ErrorTypeConstraint ErrorType = "constraint"
)

// TranscodingError represents a failure during the ffmpeg transcoding process.
type TranscodingError struct {
	JobID   string
	Message string
	Stderr  string
}

func (e *TranscodingError) Error() string {
	return fmt.Sprintf("transcoding error [job=%s]: %s", e.JobID, e.Message)
}

// SourceNotFoundError represents a permanent failure when the S3 source does not exist (404).
type SourceNotFoundError struct {
	JobID   string
	S3URI   string
	Message string
}

func (e *SourceNotFoundError) Error() string {
	return fmt.Sprintf("source not found [job=%s, uri=%s]: %s", e.JobID, e.S3URI, e.Message)
}

// ResourceConstraintError represents an unrecoverable resource issue (OOM, disk full).
type ResourceConstraintError struct {
	JobID    string
	Resource string // "disk" or "memory"
	Message  string
}

func (e *ResourceConstraintError) Error() string {
	return fmt.Sprintf("resource constraint [job=%s, resource=%s]: %s", e.JobID, e.Resource, e.Message)
}

// PermanentError wraps any error that should never be retried (corrupted file, unsupported codec, invalid path).
type PermanentError struct {
	JobID   string
	Reason  string
	Wrapped error
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error [job=%s]: %s", e.JobID, e.Reason)
}

func (e *PermanentError) Unwrap() error {
	return e.Wrapped
}

// ClassifyError determines error type for retry/DLQ decision.
// Resource constraint → exit pod. Permanent → DLQ immediately. Retryable → may retry.
func ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeRetryable // shouldn't happen, but safe default
	}

	// Resource constraint — pod must die
	var rce *ResourceConstraintError
	if errors.As(err, &rce) {
		return ErrorTypeConstraint
	}

	// Explicit permanent errors
	var pe *PermanentError
	if errors.As(err, &pe) {
		return ErrorTypePermanent
	}

	// Source not found (404) — permanent
	var snfe *SourceNotFoundError
	if errors.As(err, &snfe) {
		return ErrorTypePermanent
	}

	// Check error message patterns for classification
	msg := strings.ToLower(err.Error())

	// Permanent patterns: corrupted file, unsupported codec, invalid path
	permanentPatterns := []string{
		"unsupported codec",
		"corrupted",
		"invalid s3 path",
		"invalid s3 uri",
		"access denied",
		"forbidden",
	}
	for _, p := range permanentPatterns {
		if strings.Contains(msg, p) {
			return ErrorTypePermanent
		}
	}

	// Resource constraint patterns
	constraintPatterns := []string{
		"no space left on device",
		"out of memory",
		"oom",
		"cannot allocate memory",
	}
	for _, p := range constraintPatterns {
		if strings.Contains(msg, p) {
			return ErrorTypeConstraint
		}
	}

	// Default: retryable (network timeout, S3 503, Kafka unavailable, temp disk full)
	return ErrorTypeRetryable
}
