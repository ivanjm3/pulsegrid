package pkg

import (
	"context"
	"fmt"
	"time"
)

// RetryWithBackoff retries fn with exponential backoff.
// Delays: baseDelay * 2^attempt, capped at 16s.
// Respects context cancellation.
func RetryWithBackoff(ctx context.Context, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var lastErr error
	maxDelay := 16 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// Don't sleep after last attempt
		if attempt == maxAttempts-1 {
			break
		}

		delay := baseDelay * (1 << uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled: %w (last error: %v)", ctx.Err(), lastErr)
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", maxAttempts, lastErr)
}
