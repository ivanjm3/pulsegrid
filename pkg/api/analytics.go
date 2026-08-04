package api

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"pulsegrid/pkg/analytics"
)

// analyticsQueryTimeout bounds each of the four parallel view queries, per
// task 39.
const analyticsQueryTimeout = 5 * time.Second

// ThroughputFetcher queries analytics.v_throughput_per_minute. Satisfied by
// *pkg/analytics.Queries.
type ThroughputFetcher interface {
	FetchThroughputPerMinute(ctx context.Context) ([]analytics.ThroughputPoint, error)
}

// LatencyFetcher queries analytics.v_latency_percentiles. Satisfied by
// *pkg/analytics.Queries.
type LatencyFetcher interface {
	FetchLatencyPercentiles(ctx context.Context) ([]analytics.LatencyPercentiles, error)
}

// FailureRateFetcher queries analytics.v_failure_rate_by_class. Satisfied by
// *pkg/analytics.Queries.
type FailureRateFetcher interface {
	FetchFailureRateByClass(ctx context.Context) ([]analytics.FailureRateByClass, error)
}

// RenditionBreakdownFetcher queries analytics.v_rendition_breakdown.
// Satisfied by *pkg/analytics.Queries.
type RenditionBreakdownFetcher interface {
	FetchRenditionBreakdown(ctx context.Context) ([]analytics.RenditionBreakdown, error)
}

// AnalyticsSummaryResponse is returned by GET /analytics/summary.
type AnalyticsSummaryResponse struct {
	GeneratedAt         string                         `json:"generated_at"`
	ThroughputPerMinute []analytics.ThroughputPoint    `json:"throughput_per_minute"`
	LatencyPercentiles  []analytics.LatencyPercentiles `json:"latency_percentiles"`
	FailureRateByClass  []analytics.FailureRateByClass `json:"failure_rate_by_class"`
	RenditionBreakdown  []analytics.RenditionBreakdown `json:"rendition_breakdown"`
}

// AnalyticsSummaryHandler handles GET /analytics/summary: queries all four
// analytics materialized views in parallel, each bounded by
// analyticsQueryTimeout. Read-only — it never writes to any table.
type AnalyticsSummaryHandler struct {
	Throughput  ThroughputFetcher
	Latency     LatencyFetcher
	FailureRate FailureRateFetcher
	Rendition   RenditionBreakdownFetcher
}

// NewAnalyticsSummaryHandler returns an AnalyticsSummaryHandler wired to the
// four view fetchers.
func NewAnalyticsSummaryHandler(throughput ThroughputFetcher, latency LatencyFetcher, failureRate FailureRateFetcher, rendition RenditionBreakdownFetcher) *AnalyticsSummaryHandler {
	return &AnalyticsSummaryHandler{
		Throughput:  throughput,
		Latency:     latency,
		FailureRate: failureRate,
		Rendition:   rendition,
	}
}

func (h *AnalyticsSummaryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	w.Header().Set("X-Request-Id", requestID)

	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), analyticsQueryTimeout)
	defer cancel()

	resp := AnalyticsSummaryResponse{GeneratedAt: time.Now().UTC().Format(time.RFC3339)}

	var wg sync.WaitGroup
	ok := [4]bool{}
	wg.Add(4)

	go func() {
		defer wg.Done()
		v, err := h.Throughput.FetchThroughputPerMinute(ctx)
		if err != nil {
			log.Printf("analytics_summary request_id=%s event=view_query_failed view=throughput_per_minute error=%v", requestID, err)
			return
		}
		resp.ThroughputPerMinute, ok[0] = v, true
	}()

	go func() {
		defer wg.Done()
		v, err := h.Latency.FetchLatencyPercentiles(ctx)
		if err != nil {
			log.Printf("analytics_summary request_id=%s event=view_query_failed view=latency_percentiles error=%v", requestID, err)
			return
		}
		resp.LatencyPercentiles, ok[1] = v, true
	}()

	go func() {
		defer wg.Done()
		v, err := h.FailureRate.FetchFailureRateByClass(ctx)
		if err != nil {
			log.Printf("analytics_summary request_id=%s event=view_query_failed view=failure_rate_by_class error=%v", requestID, err)
			return
		}
		resp.FailureRateByClass, ok[2] = v, true
	}()

	go func() {
		defer wg.Done()
		v, err := h.Rendition.FetchRenditionBreakdown(ctx)
		if err != nil {
			log.Printf("analytics_summary request_id=%s event=view_query_failed view=rendition_breakdown error=%v", requestID, err)
			return
		}
		resp.RenditionBreakdown, ok[3] = v, true
	}()

	wg.Wait()

	status := http.StatusOK
	for _, o := range ok {
		if !o {
			status = http.StatusServiceUnavailable
			break
		}
	}
	writeJSON(w, status, resp)
}
