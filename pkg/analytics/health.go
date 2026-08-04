package analytics

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
)

// Pinger checks connectivity to a dependency, returning an error if it is
// unreachable. Satisfied by *pkg/queue.Pinger (Kafka) and *pgxpool.Pool
// (Postgres) — the same shape as pkg/api.Pinger, kept as an independent
// copy here rather than imported from pkg/api, so the analytics consumer's
// failure domain (task 36's design note) never depends on the API server's
// package.
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// HealthHandler handles GET /health: checks Kafka and Postgres connectivity
// for the analytics-consumer's Kubernetes liveness probe (task 42). Returns
// 200 if both are reachable, 503 if either is down.
type HealthHandler struct {
	Kafka    Pinger
	Postgres Pinger
}

// NewHealthHandler returns a HealthHandler wired to kafka and postgres
// connectivity checks.
func NewHealthHandler(kafka, postgres Pinger) *HealthHandler {
	return &HealthHandler{Kafka: kafka, Postgres: postgres}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	checks := make(map[string]string, 2)
	healthy := true

	ping := func(name string, p Pinger) {
		if err := p.Ping(ctx); err != nil {
			checks[name] = "down"
			healthy = false
			log.Printf("analytics_health event=dependency_unreachable dependency=%s error=%v", name, err)
			return
		}
		checks[name] = "ok"
	}

	ping("kafka", h.Kafka)
	ping("postgres", h.Postgres)

	status := http.StatusOK
	overall := "ok"
	if !healthy {
		status = http.StatusServiceUnavailable
		overall = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(HealthResponse{Status: overall, Checks: checks})
}
