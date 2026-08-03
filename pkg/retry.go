package pkg

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxBackoffDelay caps the exponential backoff delay computed by
// RetryWithBackoff, matching the design doc's retry policy across S3,
// Kafka, and Postgres call sites.
const MaxBackoffDelay = 16 * time.Second

// permanentError marks an error as non-retryable. See Permanent.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so RetryWithBackoff stops immediately instead of
// retrying it (e.g. S3 AccessDenied, a 404 source object, a resource
// constraint). Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// RetryWithBackoff calls fn up to maxAttempts times. Between attempts it
// waits baseDelay*2^attempt (capped at MaxBackoffDelay) via sleep before
// trying again. It stops immediately, without retrying, if fn returns an
// error wrapped with Permanent, or if ctx is already cancelled before an
// attempt starts.
//
// On exhausting all attempts, the returned error wraps the last error from
// fn with the attempt count.
func RetryWithBackoff(ctx context.Context, maxAttempts int, baseDelay time.Duration, sleep func(time.Duration), fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(uint64(1)<<uint(attempt-1))
			if delay > MaxBackoffDelay {
				delay = MaxBackoffDelay
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			sleep(delay)
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		var perm *permanentError
		if errors.As(err, &perm) {
			return perm.err
		}
		lastErr = err
	}

	return fmt.Errorf("exhausted %d attempts: %w", maxAttempts, lastErr)
}
