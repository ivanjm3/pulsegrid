package pkg

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyError_Nil(t *testing.T) {
	// nil error defaults to retryable (shouldn't happen in practice)
	if got := ClassifyError(nil); got != ErrorTypeRetryable {
		t.Fatalf("expected retryable for nil, got %s", got)
	}
}

func TestClassifyError_ResourceConstraint(t *testing.T) {
	err := &ResourceConstraintError{JobID: "j1", Resource: "disk", Message: "no space left on device"}
	if got := ClassifyError(err); got != ErrorTypeConstraint {
		t.Fatalf("expected constraint, got %s", got)
	}
}

func TestClassifyError_ResourceConstraint_OOM(t *testing.T) {
	err := &ResourceConstraintError{JobID: "j2", Resource: "memory", Message: "out of memory"}
	if got := ClassifyError(err); got != ErrorTypeConstraint {
		t.Fatalf("expected constraint, got %s", got)
	}
}

func TestClassifyError_SourceNotFound(t *testing.T) {
	err := &SourceNotFoundError{JobID: "j3", S3URI: "s3://bucket/key", Message: "404"}
	if got := ClassifyError(err); got != ErrorTypePermanent {
		t.Fatalf("expected permanent, got %s", got)
	}
}

func TestClassifyError_PermanentError(t *testing.T) {
	err := &PermanentError{JobID: "j4", Reason: "unsupported codec VP9", Wrapped: errors.New("codec")}
	if got := ClassifyError(err); got != ErrorTypePermanent {
		t.Fatalf("expected permanent, got %s", got)
	}
}

func TestClassifyError_Wrapped_ResourceConstraint(t *testing.T) {
	inner := &ResourceConstraintError{JobID: "j5", Resource: "disk", Message: "full"}
	wrapped := fmt.Errorf("download failed: %w", inner)
	if got := ClassifyError(wrapped); got != ErrorTypeConstraint {
		t.Fatalf("expected constraint through wrapping, got %s", got)
	}
}

func TestClassifyError_Wrapped_SourceNotFound(t *testing.T) {
	inner := &SourceNotFoundError{JobID: "j6", S3URI: "s3://b/k", Message: "not found"}
	wrapped := fmt.Errorf("source download: %w", inner)
	if got := ClassifyError(wrapped); got != ErrorTypePermanent {
		t.Fatalf("expected permanent through wrapping, got %s", got)
	}
}

func TestClassifyError_MessagePattern_Corrupted(t *testing.T) {
	err := errors.New("corrupted video file detected")
	if got := ClassifyError(err); got != ErrorTypePermanent {
		t.Fatalf("expected permanent for corrupted, got %s", got)
	}
}

func TestClassifyError_MessagePattern_UnsupportedCodec(t *testing.T) {
	err := errors.New("ffmpeg: Unsupported codec vp9")
	if got := ClassifyError(err); got != ErrorTypePermanent {
		t.Fatalf("expected permanent for unsupported codec, got %s", got)
	}
}

func TestClassifyError_MessagePattern_InvalidS3Path(t *testing.T) {
	err := errors.New("invalid s3 path: missing bucket")
	if got := ClassifyError(err); got != ErrorTypePermanent {
		t.Fatalf("expected permanent for invalid s3 path, got %s", got)
	}
}

func TestClassifyError_MessagePattern_NoSpace(t *testing.T) {
	err := errors.New("write: no space left on device")
	if got := ClassifyError(err); got != ErrorTypeConstraint {
		t.Fatalf("expected constraint for no space left, got %s", got)
	}
}

func TestClassifyError_MessagePattern_OOM(t *testing.T) {
	err := errors.New("cannot allocate memory")
	if got := ClassifyError(err); got != ErrorTypeConstraint {
		t.Fatalf("expected constraint for cannot allocate memory, got %s", got)
	}
}

func TestClassifyError_TransientDefault(t *testing.T) {
	// Generic network error → retryable
	err := errors.New("connection reset by peer")
	if got := ClassifyError(err); got != ErrorTypeRetryable {
		t.Fatalf("expected retryable for network error, got %s", got)
	}
}

func TestClassifyError_S3_503(t *testing.T) {
	err := errors.New("s3 SlowDown: reduce your request rate")
	if got := ClassifyError(err); got != ErrorTypeRetryable {
		t.Fatalf("expected retryable for s3 503, got %s", got)
	}
}

func TestClassifyError_KafkaUnavailable(t *testing.T) {
	err := errors.New("kafka broker unavailable: connection refused")
	if got := ClassifyError(err); got != ErrorTypeRetryable {
		t.Fatalf("expected retryable for kafka unavailable, got %s", got)
	}
}
