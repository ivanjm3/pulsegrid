package analytics

import (
	"context"
	"log/slog"

	"pulsegrid/pkg/queue"
)

// LogEventHandler is a placeholder EventHandler that logs each lifecycle
// event instead of writing to Postgres. It exists so the consumer loop,
// offset-commit gating, and SIGTERM handling can run end-to-end before the
// analytics.job_lifecycle_events Postgres sink (task 37) exists.
type LogEventHandler struct {
	logger *slog.Logger
}

// NewLogEventHandler returns a LogEventHandler that logs to logger.
func NewLogEventHandler(logger *slog.Logger) *LogEventHandler {
	return &LogEventHandler{logger: logger}
}

// HandleEvent logs event and always succeeds.
func (h *LogEventHandler) HandleEvent(ctx context.Context, event queue.JobLifecycleEvent) error {
	h.logger.Info("lifecycle_event_received",
		"job_id", event.JobID,
		"event_type", event.EventType,
		"pod_id", event.PodID,
		"timestamp", event.Timestamp,
	)
	return nil
}
