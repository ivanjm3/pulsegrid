// Package worker (this file): job completion, retry, and dead-letter-queue
// handling for the worker pod's processing loop (task 18).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/smithy-go"

	"pulsegrid/pkg"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
)

// ErrorClass categorizes a job processing failure for retry/DLQ routing, per
// the design doc's error classification table.
type ErrorClass string

const (
	// ErrorClassRetryable is a transient failure (network timeout, S3
	// 503/SlowDown, Kafka unavailable): retry up to maxRetries before DLQ.
	ErrorClassRetryable ErrorClass = "retryable"
	// ErrorClassPermanent is a non-retryable failure (corrupted video,
	// unsupported codec, missing source, invalid S3 path): DLQ immediately.
	ErrorClassPermanent ErrorClass = "permanent"
	// ErrorClassConstraint is a pod-fatal resource failure (out of disk,
	// OOM): the pod must exit immediately, not retry or DLQ.
	ErrorClassConstraint ErrorClass = "constraint"
)

// permanentAPIErrorCodes are S3 error codes that indicate a permanent,
// non-retryable failure rather than a transient one.
var permanentAPIErrorCodes = map[string]bool{
	"NoSuchKey":             true,
	"NotFound":              true,
	"AccessDenied":          true,
	"NoSuchBucket":          true,
	"InvalidAccessKeyId":    true,
	"SignatureDoesNotMatch": true,
}

// permanentFFmpegSignals are substrings of ffmpeg stderr that indicate a
// permanent failure (bad input, unsupported feature) rather than a
// transient one (e.g. a timeout, which is retryable).
var permanentFFmpegSignals = []string{
	"unsupported codec",
	"invalid data found when processing input",
	"moov atom not found",
	"unknown encoder",
	"unrecognized option",
	"no such filter",
}

// ClassifyError determines how a job processing failure should be handled:
// pod-fatal, immediate DLQ, or retry.
func ClassifyError(err error) ErrorClass {
	var rcErr *pkg.ResourceConstraintError
	if errors.As(err, &rcErr) {
		return ErrorClassConstraint
	}

	if isPermanentError(err) {
		return ErrorClassPermanent
	}

	return ErrorClassRetryable
}

func isPermanentError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && permanentAPIErrorCodes[apiErr.ErrorCode()] {
		return true
	}

	var tErr *pkg.TranscodingError
	if errors.As(err, &tErr) {
		lower := strings.ToLower(tErr.Stderr)
		for _, signal := range permanentFFmpegSignals {
			if strings.Contains(lower, signal) {
				return true
			}
		}
	}

	msg := err.Error()
	return strings.Contains(msg, "source object not found") || strings.Contains(msg, "invalid s3 uri")
}

// Outcome describes the action HandleFailure took in response to a job
// processing failure.
type Outcome string

const (
	OutcomeRetried     Outcome = "retried"
	OutcomeDLQd        Outcome = "dlq"
	OutcomeConstrained Outcome = "constrained"
)

// MaxRetries matches the design doc: after 3 failed retries a job moves to
// the DLQ.
const MaxRetries = 3

// RetryPublisher re-enqueues a job to the transcoding-jobs topic.
type RetryPublisher interface {
	EnqueueJob(ctx context.Context, job pkg.Job) error
}

// DLQPublisher publishes a job to the dead-letter topic.
type DLQPublisher interface {
	SendDLQ(ctx context.Context, msg queue.DLQMessage) error
}

// StatusRecorder records a job status event, matching *store.Store's
// signature.
type StatusRecorder interface {
	RecordStatusEvent(ctx context.Context, jobID, eventType string, eventData map[string]any, podID string) error
}

// LifecycleHandler implements job completion, retry, and DLQ routing (task
// 18): on success it records completion; on failure it classifies the error
// and either re-enqueues with an incremented retry count, publishes to the
// DLQ, or (for resource constraints) signals the caller to exit the pod.
type LifecycleHandler struct {
	retry   RetryPublisher
	dlq     DLQPublisher
	store   StatusRecorder
	metrics *metrics.WorkerMetrics
	podID   string
}

// NewLifecycleHandler returns a LifecycleHandler wired to retry, dlq, and
// store, reporting events under podID.
func NewLifecycleHandler(retry RetryPublisher, dlq DLQPublisher, store StatusRecorder, m *metrics.WorkerMetrics, podID string) *LifecycleHandler {
	return &LifecycleHandler{retry: retry, dlq: dlq, store: store, metrics: m, podID: podID}
}

// HandleSuccess records a job's successful completion.
func (h *LifecycleHandler) HandleSuccess(ctx context.Context, msg queue.JobMessage) error {
	h.metrics.IncJobCompleted()
	return h.store.RecordStatusEvent(ctx, msg.JobID, "job_completed", nil, h.podID)
}

// HandleFailure classifies procErr and routes msg accordingly: retry
// (re-enqueue with retry_count+1), DLQ (permanent error, or retry_count
// already at MaxRetries), or constrained (pod-fatal, caller must exit). The
// returned error is non-nil only when the routing action itself (publish to
// Kafka) failed — the caller should then leave the Kafka offset uncommitted
// so the message is redelivered.
func (h *LifecycleHandler) HandleFailure(ctx context.Context, msg queue.JobMessage, procErr error) (Outcome, error) {
	class := ClassifyError(procErr)
	h.metrics.IncTranscodeFailure(string(class))

	if class == ErrorClassConstraint {
		if err := h.recordEvent(ctx, msg.JobID, "pod_resource_constrained", procErr, class); err != nil {
			log.Printf("event=record_status_event_failed job_id=%s error=%v", msg.JobID, err)
		}
		return OutcomeConstrained, nil
	}

	if class == ErrorClassPermanent || msg.RetryCount >= MaxRetries {
		if err := h.sendToDLQ(ctx, msg, procErr); err != nil {
			return "", fmt.Errorf("handle failure: send to dlq: %w", err)
		}
		if err := h.recordEvent(ctx, msg.JobID, "job_failed", procErr, class); err != nil {
			log.Printf("event=record_status_event_failed job_id=%s error=%v", msg.JobID, err)
		}
		return OutcomeDLQd, nil
	}

	if err := h.retryEnqueue(ctx, msg); err != nil {
		return "", fmt.Errorf("handle failure: retry enqueue: %w", err)
	}
	if err := h.recordEvent(ctx, msg.JobID, "job_failed", procErr, class); err != nil {
		log.Printf("event=record_status_event_failed job_id=%s error=%v", msg.JobID, err)
	}
	return OutcomeRetried, nil
}

func (h *LifecycleHandler) retryEnqueue(ctx context.Context, msg queue.JobMessage) error {
	job := jobFromMessage(msg)
	job.RetryCount = msg.RetryCount + 1
	return h.retry.EnqueueJob(ctx, job)
}

func (h *LifecycleHandler) sendToDLQ(ctx context.Context, msg queue.JobMessage, procErr error) error {
	now := time.Now().UTC().Format(time.RFC3339)
	dlqMsg := queue.DLQMessage{
		JobMessage:        msg,
		DLQEntryTimestamp: now,
		FailureReason:     procErr.Error(),
		FailureTimestamp:  now,
		PodID:             h.podID,
		StderrSnippet:     stderrSnippet(procErr),
	}
	return h.dlq.SendDLQ(ctx, dlqMsg)
}

func (h *LifecycleHandler) recordEvent(ctx context.Context, jobID, eventType string, procErr error, class ErrorClass) error {
	return h.store.RecordStatusEvent(ctx, jobID, eventType, map[string]any{
		"error":      procErr.Error(),
		"error_type": string(class),
	}, h.podID)
}

// stderrSnippet extracts the first 500 chars of captured ffmpeg stderr from
// procErr, if it is a *pkg.TranscodingError. Matches the design doc's DLQ
// message schema stderr_snippet field.
func stderrSnippet(procErr error) string {
	var tErr *pkg.TranscodingError
	if !errors.As(procErr, &tErr) {
		return ""
	}
	s := tErr.Stderr
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}

// jobFromMessage reconstructs the pkg.Job fields needed to re-publish a job
// message from its decoded JobMessage form.
func jobFromMessage(msg queue.JobMessage) pkg.Job {
	submitted, _ := time.Parse(time.RFC3339, msg.SubmittedTimestamp)
	return pkg.Job{
		ID:             msg.JobID,
		SourceS3URI:    msg.SourceS3URI,
		Renditions:     msg.Renditions,
		OutputS3Prefix: msg.OutputS3Prefix,
		RetryCount:     msg.RetryCount,
		SubmissionTime: submitted,
	}
}
