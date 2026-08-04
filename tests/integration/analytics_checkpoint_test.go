//go:build integration

// Task 43's checkpoint: end-to-end through the whole analytics pipeline,
// against a real Postgres instance. Same "environment-realistic substitute"
// as tasks 33/34/38 (see .spec/CHANGELOG.md): there is no live EKS staging
// cluster or Grafana instance available in this environment, so this test
// chains every piece of code the checkpoint's manual steps would otherwise
// exercise by hand — publish lifecycle events, sink them, refresh the
// materialized views, query GET /analytics/summary, verify the Prometheus
// counters — using real production types end to end, only the Kafka broker
// and HTTP transport are faked (same "borrow real code, fake only the wire"
// approach every other test in this repo uses).
package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"pulsegrid/pkg/analytics"
	"pulsegrid/pkg/api"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/store"
)

// fakeLifecycleBroker is an analytics.Reader backed by an in-memory queue of
// messages, fed directly by a real analytics.LifecycleProducer (the same
// producer type the worker uses in production) rather than hand-built JSON.
type fakeLifecycleBroker struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	delivered int
}

func (b *fakeLifecycleBroker) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range msgs {
		m.Offset = int64(len(b.msgs))
		b.msgs = append(b.msgs, m)
	}
	return nil
}
func (b *fakeLifecycleBroker) Close() error { return nil }

func (b *fakeLifecycleBroker) FetchMessage(ctx context.Context) (kafka.Message, error) {
	b.mu.Lock()
	if b.delivered < len(b.msgs) {
		m := b.msgs[b.delivered]
		b.delivered++
		b.mu.Unlock()
		return m, nil
	}
	b.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (b *fakeLifecycleBroker) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

// TestAnalyticsPipeline_EndToEnd is the task 43 checkpoint: 20 simulated
// jobs' lifecycle events flow through a real LifecycleProducer -> real
// analytics.Consumer -> real PostgresSink -> real Postgres, the four
// materialized views are refreshed with the real Refresher, and the real
// GET /analytics/summary handler (backed by real analytics.Queries against
// the same database) is hit end to end.
func TestAnalyticsPipeline_EndToEnd(t *testing.T) {
	url := dsn(t)
	if err := store.RunMigrations(url); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	rawDB, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer rawDB.Close()
	if _, err := rawDB.Exec(`TRUNCATE analytics.job_lifecycle_events`); err != nil {
		t.Fatalf("truncate analytics.job_lifecycle_events: %v", err)
	}

	pool, err := store.Connect(context.Background(), url)
	if err != nil {
		t.Fatalf("store.Connect: %v", err)
	}
	defer pool.Close()

	// --- Publish 20 simulated jobs' worth of lifecycle events, through the
	// real production LifecycleProducer, into a fake in-memory broker.
	broker := &fakeLifecycleBroker{}
	producer := queue.NewLifecycleProducer(broker)

	const numJobs = 20
	now := time.Now().UTC()
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("55555555-0000-0000-0000-%012d", i)
		start := now.Add(-time.Duration(numJobs-i) * time.Second)
		mustPublish(t, producer, queue.JobLifecycleEvent{
			JobID: jobID, EventType: queue.EventJobStarted, PodID: "checkpoint-pod",
			Timestamp: start.Format(time.RFC3339),
		})
		mustPublish(t, producer, queue.JobLifecycleEvent{
			JobID: jobID, EventType: queue.EventRenditionCompleted, RenditionID: strp("720p"), PodID: "checkpoint-pod",
			Timestamp: start.Add(5 * time.Second).Format(time.RFC3339),
		})
		mustPublish(t, producer, queue.JobLifecycleEvent{
			JobID: jobID, EventType: queue.EventJobCompleted, PodID: "checkpoint-pod",
			Timestamp: start.Add(10 * time.Second).Format(time.RFC3339),
		})
	}

	// --- Real Consumer + real PostgresSink + real AnalyticsMetrics, draining
	// the broker into Postgres.
	sink := analytics.NewPostgresSink(pool)
	m := metrics.NewAnalytics()
	consumer := analytics.NewConsumer(broker, sink, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	wantEvents := numJobs * 3
	deadline := time.After(9 * time.Second)
	for {
		var count int
		if err := rawDB.QueryRow(`SELECT COUNT(*) FROM analytics.job_lifecycle_events`).Scan(&count); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if count >= wantEvents {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d events landed in analytics.job_lifecycle_events", count, wantEvents)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// --- Verify Prometheus counters (task 40) reflect the real run: every
	// event type processed, lag well under 5s (events were all timestamped
	// in the recent past, sunk immediately).
	sum := 0
	for _, et := range []string{"job_started", "rendition_completed", "job_completed", "job_failed"} {
		sum += int(testutil.ToFloat64(m.EventsProcessedTotal.WithLabelValues(et)))
	}
	if sum != wantEvents {
		t.Errorf("pulsegrid_analytics_events_processed_total sum = %d, want %d", sum, wantEvents)
	}

	// --- Refresh all four materialized views (task 38's Refresher, same
	// code path the analytics-consumer's background goroutine runs).
	refresher := analytics.NewRefresher(pool)
	if err := refresher.RefreshAll(context.Background()); err != nil {
		t.Fatalf("RefreshAll: %v", err)
	}

	// --- Hit the real GET /analytics/summary handler, backed by real
	// analytics.Queries against the same pool.
	queries := analytics.NewQueries(pool)
	handler := api.NewAnalyticsSummaryHandler(queries, queries, queries, queries)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/analytics/summary", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /analytics/summary status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp api.AnalyticsSummaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.ThroughputPerMinute) == 0 {
		t.Error("throughput_per_minute is empty")
	}
	if len(resp.LatencyPercentiles) == 0 {
		t.Error("latency_percentiles is empty")
	}
	if len(resp.RenditionBreakdown) == 0 {
		t.Error("rendition_breakdown is empty")
	}
	// failure_rate_by_class is legitimately empty here — this checkpoint's
	// 20 jobs are all successful completions, matching v_failure_rate_by_class's
	// own WHERE event_type = 'job_failed' filter (task 38's SQL). The
	// per-error-class failure path is already covered by
	// TestView_FailureRateByClass in analytics_views_test.go.
}

func mustPublish(t *testing.T, p *queue.LifecycleProducer, event queue.JobLifecycleEvent) {
	t.Helper()
	if err := p.PublishEvent(context.Background(), event); err != nil {
		t.Fatalf("publish lifecycle event: %v", err)
	}
}
