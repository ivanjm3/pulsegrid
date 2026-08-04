package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"pulsegrid/pkg/analytics"
)

// fakeThroughput, fakeLatency, fakeFailureRate, fakeRendition each record
// whether they were called and can simulate an error or a delay that
// outlasts the handler's context timeout, to exercise the partial-failure
// (503) path.
type fakeThroughput struct {
	called atomic.Bool
	delay  time.Duration
	err    error
	rows   []analytics.ThroughputPoint
}

func (f *fakeThroughput) FetchThroughputPerMinute(ctx context.Context) ([]analytics.ThroughputPoint, error) {
	f.called.Store(true)
	return waitOrErr(ctx, f.delay, f.rows, f.err)
}

type fakeLatency struct {
	called atomic.Bool
	delay  time.Duration
	err    error
	rows   []analytics.LatencyPercentiles
}

func (f *fakeLatency) FetchLatencyPercentiles(ctx context.Context) ([]analytics.LatencyPercentiles, error) {
	f.called.Store(true)
	return waitOrErr(ctx, f.delay, f.rows, f.err)
}

type fakeFailureRate struct {
	called atomic.Bool
	delay  time.Duration
	err    error
	rows   []analytics.FailureRateByClass
}

func (f *fakeFailureRate) FetchFailureRateByClass(ctx context.Context) ([]analytics.FailureRateByClass, error) {
	f.called.Store(true)
	return waitOrErr(ctx, f.delay, f.rows, f.err)
}

type fakeRendition struct {
	called atomic.Bool
	delay  time.Duration
	err    error
	rows   []analytics.RenditionBreakdown
}

func (f *fakeRendition) FetchRenditionBreakdown(ctx context.Context) ([]analytics.RenditionBreakdown, error) {
	f.called.Store(true)
	return waitOrErr(ctx, f.delay, f.rows, f.err)
}

func waitOrErr[T any](ctx context.Context, delay time.Duration, rows T, err error) (T, error) {
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
	if err != nil {
		var zero T
		return zero, err
	}
	return rows, nil
}

func TestAnalyticsSummaryHandler_AllSucceed_Returns200WithFullPayload(t *testing.T) {
	throughput := &fakeThroughput{rows: []analytics.ThroughputPoint{{JobsCompleted: 3}}}
	latency := &fakeLatency{rows: []analytics.LatencyPercentiles{{}}}
	failureRate := &fakeFailureRate{rows: []analytics.FailureRateByClass{{FailureCount: 1}}}
	rendition := &fakeRendition{rows: []analytics.RenditionBreakdown{{CompletedCount: 2}}}
	h := NewAnalyticsSummaryHandler(throughput, latency, failureRate, rendition)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	for name, called := range map[string]bool{
		"throughput":   throughput.called.Load(),
		"latency":      latency.called.Load(),
		"failure_rate": failureRate.called.Load(),
		"rendition":    rendition.called.Load(),
	} {
		if !called {
			t.Errorf("%s fetcher was never called", name)
		}
	}

	var resp AnalyticsSummaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.GeneratedAt == "" {
		t.Error("generated_at is empty")
	}
	if len(resp.ThroughputPerMinute) != 1 || len(resp.LatencyPercentiles) != 1 ||
		len(resp.FailureRateByClass) != 1 || len(resp.RenditionBreakdown) != 1 {
		t.Errorf("expected all four sections populated, got %+v", resp)
	}
}

func TestAnalyticsSummaryHandler_QueriesRunInParallel(t *testing.T) {
	// Each fetcher blocks for slightly under the shared 5s timeout. If they
	// ran sequentially, four such delays would total ~800ms; run in
	// parallel, the whole handler should return in ~200ms plus overhead.
	const perCallDelay = 200 * time.Millisecond
	throughput := &fakeThroughput{delay: perCallDelay}
	latency := &fakeLatency{delay: perCallDelay}
	failureRate := &fakeFailureRate{delay: perCallDelay}
	rendition := &fakeRendition{delay: perCallDelay}
	h := NewAnalyticsSummaryHandler(throughput, latency, failureRate, rendition)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)

	start := time.Now()
	h.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if elapsed > 3*perCallDelay {
		t.Errorf("elapsed = %v, expected well under %v if queries ran in parallel", elapsed, 3*perCallDelay)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAnalyticsSummaryHandler_OneViewTimesOut_Returns503WithPartialData(t *testing.T) {
	throughput := &fakeThroughput{rows: []analytics.ThroughputPoint{{JobsCompleted: 3}}}
	latency := &fakeLatency{delay: 10 * time.Second} // exceeds analyticsQueryTimeout
	failureRate := &fakeFailureRate{rows: []analytics.FailureRateByClass{{FailureCount: 1}}}
	rendition := &fakeRendition{rows: []analytics.RenditionBreakdown{{CompletedCount: 2}}}
	h := NewAnalyticsSummaryHandler(throughput, latency, failureRate, rendition)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	var resp AnalyticsSummaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.ThroughputPerMinute) != 1 || len(resp.FailureRateByClass) != 1 || len(resp.RenditionBreakdown) != 1 {
		t.Errorf("expected the three successful sections still populated, got %+v", resp)
	}
	if resp.LatencyPercentiles != nil {
		t.Errorf("expected latency_percentiles empty on timeout, got %+v", resp.LatencyPercentiles)
	}
}

func TestAnalyticsSummaryHandler_OneViewErrors_Returns503(t *testing.T) {
	throughput := &fakeThroughput{rows: []analytics.ThroughputPoint{{JobsCompleted: 3}}}
	latency := &fakeLatency{rows: []analytics.LatencyPercentiles{{}}}
	failureRate := &fakeFailureRate{err: errors.New("db connection reset")}
	rendition := &fakeRendition{rows: []analytics.RenditionBreakdown{{CompletedCount: 2}}}
	h := NewAnalyticsSummaryHandler(throughput, latency, failureRate, rendition)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAnalyticsSummaryHandler_WrongMethod_Returns405(t *testing.T) {
	h := NewAnalyticsSummaryHandler(&fakeThroughput{}, &fakeLatency{}, &fakeFailureRate{}, &fakeRendition{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/analytics/summary", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
