package pkg

import (
	"context"
	"errors"
	"testing"
	"time"
)

func noSleep(time.Duration) {}

func TestRetryWithBackoff_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), 5, time.Second, noSleep, func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("RetryWithBackoff returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}
}

func TestRetryWithBackoff_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	var sleeps []time.Duration
	err := RetryWithBackoff(context.Background(), 5, time.Second, func(d time.Duration) { sleeps = append(sleeps, d) }, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryWithBackoff returned error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want 3", calls)
	}
	want := []time.Duration{1 * time.Second, 2 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
	for i := range want {
		if sleeps[i] != want[i] {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], want[i])
		}
	}
}

func TestRetryWithBackoff_AllAttemptsFail_ReturnsWrappedError(t *testing.T) {
	calls := 0
	sentinel := errors.New("persistent failure")
	err := RetryWithBackoff(context.Background(), 4, time.Millisecond, noSleep, func(ctx context.Context) error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped %v", err, sentinel)
	}
	if calls != 4 {
		t.Fatalf("fn called %d times, want 4", calls)
	}
}

func TestRetryWithBackoff_PermanentError_NoRetry(t *testing.T) {
	calls := 0
	sentinel := errors.New("access denied")
	err := RetryWithBackoff(context.Background(), 5, time.Second, func(time.Duration) { t.Fatal("sleep should not be called for a permanent error") }, func(ctx context.Context) error {
		calls++
		return Permanent(sentinel)
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1 (no retry on permanent error)", calls)
	}
}

func TestRetryWithBackoff_ContextCancelled_StopsBeforeNextAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := RetryWithBackoff(ctx, 5, time.Second, func(time.Duration) {
		cancel()
	}, func(ctx context.Context) error {
		calls++
		return errors.New("transient")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 2 {
		t.Fatalf("fn called %d times, want 2 (cancelled after first retry's sleep)", calls)
	}
}

func TestRetryWithBackoff_ExponentialDelay(t *testing.T) {
	var sleeps []time.Duration
	_ = RetryWithBackoff(context.Background(), 5, 100*time.Millisecond, func(d time.Duration) { sleeps = append(sleeps, d) }, func(ctx context.Context) error {
		return errors.New("transient")
	})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	if len(sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
	for i := range want {
		if sleeps[i] != want[i] {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], want[i])
		}
	}
}

func TestRetryWithBackoff_DelayCappedAtMax(t *testing.T) {
	var sleeps []time.Duration
	_ = RetryWithBackoff(context.Background(), 8, 10*time.Second, func(d time.Duration) { sleeps = append(sleeps, d) }, func(ctx context.Context) error {
		return errors.New("transient")
	})
	// 10s, 20s(->16s cap), 40s(->16s cap), ...
	if len(sleeps) != 7 {
		t.Fatalf("sleeps = %v, want 7 entries", sleeps)
	}
	if sleeps[0] != 10*time.Second {
		t.Errorf("sleep[0] = %v, want 10s (below cap)", sleeps[0])
	}
	for i := 1; i < len(sleeps); i++ {
		if sleeps[i] != MaxBackoffDelay {
			t.Errorf("sleep[%d] = %v, want capped at %v", i, sleeps[i], MaxBackoffDelay)
		}
	}
}
