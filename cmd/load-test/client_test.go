package main

import (
	"context"
	"encoding/json"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulsegrid/pkg"
)

func TestBuildUploadRequest(t *testing.T) {
	renditions := []pkg.Rendition{{ID: "720p", Codec: "h264", BitrateKbps: 2500, Width: 1280, Height: 720}}
	req, err := buildUploadRequest(context.Background(), "http://example.test", "clip.mp4", 1024, renditions)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Errorf("Method = %s, want POST", req.Method)
	}
	if req.URL.String() != "http://example.test/videos/upload" {
		t.Errorf("URL = %s", req.URL.String())
	}

	mediaType, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		t.Fatalf("Content-Type = %q, err = %v", req.Header.Get("Content-Type"), err)
	}

	mr := multipart.NewReader(req.Body, params["boundary"])
	form, err := mr.ReadForm(10 << 20)
	if err != nil {
		t.Fatalf("ReadForm: %v", err)
	}

	if form.Value["source_name"][0] != "clip.mp4" {
		t.Errorf("source_name = %v", form.Value["source_name"])
	}

	var gotRenditions []pkg.Rendition
	if err := json.Unmarshal([]byte(form.Value["renditions"][0]), &gotRenditions); err != nil {
		t.Fatalf("unmarshal renditions: %v", err)
	}
	if len(gotRenditions) != 1 || gotRenditions[0].ID != "720p" {
		t.Errorf("renditions = %+v", gotRenditions)
	}

	if len(form.File["video"]) != 1 {
		t.Fatalf("expected one video file part, got %d", len(form.File["video"]))
	}
	if form.File["video"][0].Size != 1024 {
		t.Errorf("video size = %d, want 1024", form.File["video"][0].Size)
	}
}

func TestSubmitJob_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(submitResponse{JobID: "job-123", SubmissionTime: time.Now().Format(time.RFC3339)})
	}))
	defer srv.Close()

	jobID, submittedAt, err := submitJob(context.Background(), srv.Client(), srv.URL, "clip.mp4", 100, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if jobID != "job-123" {
		t.Errorf("jobID = %q, want job-123", jobID)
	}
	if submittedAt.IsZero() {
		t.Error("submittedAt should be recorded")
	}
}

func TestSubmitJob_NonAcceptedStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer srv.Close()

	_, _, err := submitJob(context.Background(), srv.Client(), srv.URL, "clip.mp4", 100, nil)
	if err == nil {
		t.Fatal("expected error for non-202 response")
	}
}

func TestPollJob_CompletesImmediately(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(statusResponse{Status: "completed"})
	}))
	defer srv.Close()

	submittedAt := time.Now()
	result := pollJob(context.Background(), srv.Client(), srv.URL, "job-123", submittedAt, 10*time.Millisecond, time.Second)

	if !result.Succeeded {
		t.Errorf("expected success, got err=%v", result.Err)
	}
	if result.SubmittedAt != submittedAt {
		t.Errorf("SubmittedAt not preserved")
	}
	if result.CompletedAt.Before(submittedAt) {
		t.Errorf("CompletedAt should be after SubmittedAt")
	}
}

func TestPollJob_EventuallyCompletes(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		status := "processing"
		if calls >= 3 {
			status = "completed"
		}
		json.NewEncoder(w).Encode(statusResponse{Status: status})
	}))
	defer srv.Close()

	result := pollJob(context.Background(), srv.Client(), srv.URL, "job-123", time.Now(), 5*time.Millisecond, time.Second)
	if !result.Succeeded {
		t.Errorf("expected eventual success, got err=%v", result.Err)
	}
	if calls < 3 {
		t.Errorf("expected at least 3 polls, got %d", calls)
	}
}

func TestPollJob_Failed(t *testing.T) {
	reason := "unsupported codec"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(statusResponse{Status: "failed", FailureReason: &reason})
	}))
	defer srv.Close()

	result := pollJob(context.Background(), srv.Client(), srv.URL, "job-123", time.Now(), 5*time.Millisecond, time.Second)
	if result.Succeeded {
		t.Error("expected failure result")
	}
	if result.Err == nil {
		t.Error("expected error describing failure reason")
	}
}

func TestPollJob_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(statusResponse{Status: "processing"})
	}))
	defer srv.Close()

	result := pollJob(context.Background(), srv.Client(), srv.URL, "job-123", time.Now(), 5*time.Millisecond, 20*time.Millisecond)
	if result.Succeeded {
		t.Error("expected timeout to be reported as failure")
	}
	if result.Err == nil {
		t.Error("expected timeout error")
	}
}
