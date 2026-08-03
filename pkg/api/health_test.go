package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakePinger is a test double for Pinger.
type fakePinger struct {
	err error
}

func (f *fakePinger) Ping(ctx context.Context) error { return f.err }

func TestHealthHandler_AllHealthy(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, &fakePinger{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp HealthResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)
	if resp.Status != "ok" {
		t.Errorf("status = %q, want %q", resp.Status, "ok")
	}
	for _, dep := range []string{"kafka", "postgres", "s3"} {
		if resp.Checks[dep] != "ok" {
			t.Errorf("checks[%s] = %q, want %q", dep, resp.Checks[dep], "ok")
		}
	}
}

func TestHealthHandler_KafkaDown(t *testing.T) {
	h := NewHealthHandler(&fakePinger{err: errors.New("dial tcp: connection refused")}, &fakePinger{}, &fakePinger{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var resp HealthResponse
	decodeJSON(t, rec.Body.Bytes(), &resp)
	if resp.Status != "unhealthy" {
		t.Errorf("status = %q, want %q", resp.Status, "unhealthy")
	}
	if resp.Checks["kafka"] != "down" {
		t.Errorf("checks[kafka] = %q, want %q", resp.Checks["kafka"], "down")
	}
	if resp.Checks["postgres"] != "ok" || resp.Checks["s3"] != "ok" {
		t.Errorf("checks = %+v, want postgres/s3 ok", resp.Checks)
	}
}

func TestHealthHandler_PostgresDown(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{err: errors.New("connection refused")}, &fakePinger{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthHandler_S3Down(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, &fakePinger{err: errors.New("no such bucket")})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthHandler_MethodNotAllowed(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, &fakePinger{})

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
