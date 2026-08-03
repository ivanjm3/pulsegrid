package api

import (
	"context"
	"log"
	"net/http"
)

// Pinger checks connectivity to a dependency, returning an error if it is
// unreachable. Satisfied by *pkg/queue.Pinger (Kafka), *pgxpool.Pool
// (Postgres), and *pkg/storage.BucketPinger (S3).
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// HealthHandler handles GET /health: checks Kafka, Postgres, and S3
// connectivity for Kubernetes liveness/readiness probes. Returns 200 if all
// dependencies are reachable, 503 if any is down.
type HealthHandler struct {
	Kafka    Pinger
	Postgres Pinger
	S3       Pinger
}

// NewHealthHandler returns a HealthHandler wired to kafka, postgres, and s3
// connectivity checks.
func NewHealthHandler(kafka, postgres, s3 Pinger) *HealthHandler {
	return &HealthHandler{Kafka: kafka, Postgres: postgres, S3: s3}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", "", "")
		return
	}

	ctx := r.Context()
	checks := make(map[string]string, 3)
	healthy := true

	ping := func(name string, p Pinger) {
		if err := p.Ping(ctx); err != nil {
			checks[name] = "down"
			healthy = false
			log.Printf("health event=dependency_unreachable dependency=%s error=%v", name, err)
			return
		}
		checks[name] = "ok"
	}

	ping("kafka", h.Kafka)
	ping("postgres", h.Postgres)
	ping("s3", h.S3)

	status := http.StatusOK
	overall := "ok"
	if !healthy {
		status = http.StatusServiceUnavailable
		overall = "unhealthy"
	}

	writeJSON(w, status, HealthResponse{Status: overall, Checks: checks})
}
