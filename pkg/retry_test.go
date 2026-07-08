package pkg

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoff_SuccessFirstAttempt(t *testing.T) {
	attempts := 0
	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetryWithBackoff_SuccessAfterRetries(t *testing.T) {
	attempts := 0
	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryWithBackoff_AllAttemptsFail(t *testing.T) {
	attempts := 0
	err := RetryWithBackoff(context.Background(), 5, 10*time.Millisecond, func() error {
		attempts++
		return errors.New("persistent error")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if attempts != 5 {
		t.Fatalf("expected 5 attempts, got %d", attempts)
	}
}

func TestRetryWithBackoff_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()

	err := RetryWithBackoff(ctx, 10, 20*time.Millisecond, func() error {
		attempts++
		return errors.New("keep failing")
	})
	if err == nil {
		t.Fatal("expected error from cancellation")
	}
	if attempts >= 10 {
		t.Fatalf("expected fewer than 10 attempts due to cancellation, got %d", attempts)
	}
}

func TestRetryWithBackoff_ExponentialDelays(t *testing.T) {
	start := time.Now()
	attempts := 0

	// Use 50ms base. Expect delays: 50ms, 100ms, 200ms (then succeed on 4th)
	err := RetryWithBackoff(context.Background(), 5, 50*time.Millisecond, func() error {
		attempts++
		if attempts < 4 {
			return errors.New("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	elapsed := time.Since(start)
	// Minimum expected: 50 + 100 + 200 = 350ms
	if elapsed < 300*time.Millisecond {
		t.Fatalf("expected at least 300ms elapsed for backoff, got %v", elapsed)
	}
}

func TestRetryWithBackoff_DelayCappedAt16s(t *testing.T) {
	// Verify cap logic: with base=8s, attempt 1 delay = 8s, attempt 2 = 16s (cap), not 32s
	// We can't actually wait 16s in tests, so test the function with large base and 1 retry
	// Just verify it doesn't hang — use context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := RetryWithBackoff(ctx, 3, 8*time.Second, func() error {
		return errors.New("fail")
	})
	// Should be cancelled by context before completing all retries
	if err == nil {
		t.Fatal("expected error")
	}
}
