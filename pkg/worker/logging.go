// Package worker (this file): structured JSON error logging (task 20).
package worker

import (
	"io"
	"log/slog"
)

// NewLogger returns a slog.Logger that writes one structured JSON record per
// line to w. slog.JSONHandler adds an RFC 3339 "time" field to every record
// automatically; callers attach the remaining required context (job_id,
// pod_id, error_message, event_type) via LogJobError.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil))
}

// LogJobError emits a structured error log line for a job processing
// failure, per the design doc: timestamp (added by the handler), job_id,
// pod_id, error_message, event_type, retry_count, and error_type.
// ffmpegStderr, if non-empty, is truncated to its first 500 characters.
func LogJobError(logger *slog.Logger, eventType, jobID, podID string, err error, retryCount int, errorType ErrorClass, ffmpegStderr string) {
	if len(ffmpegStderr) > 500 {
		ffmpegStderr = ffmpegStderr[:500]
	}
	logger.Error(eventType,
		"job_id", jobID,
		"pod_id", podID,
		"error_message", err.Error(),
		"event_type", eventType,
		"retry_count", retryCount,
		"error_type", string(errorType),
		"ffmpeg_stderr", ffmpegStderr,
	)
}
