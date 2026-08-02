package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// multipartRequest builds a POST /videos/upload request. Pass videoBytes as
// nil to omit the video field entirely.
func multipartRequest(t *testing.T, fields map[string]string, videoBytes []byte) *http.Request {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	if videoBytes != nil {
		fw, err := w.CreateFormFile("video", "source.mp4")
		if err != nil {
			t.Fatalf("create form file: %v", err)
		}
		if _, err := fw.Write(videoBytes); err != nil {
			t.Fatalf("write video bytes: %v", err)
		}
	}

	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatalf("write field %s: %v", k, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

func TestUploadHandler_ValidRequest_Returns202(t *testing.T) {
	h := NewUploadHandler()
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp UploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.JobID == "" {
		t.Error("expected non-empty job_id")
	}
	if !strings.Contains(resp.StatusURI, resp.JobID) {
		t.Errorf("status_uri %q does not reference job_id %q", resp.StatusURI, resp.JobID)
	}
	if resp.SubmissionTime == "" {
		t.Error("expected non-empty submission_time")
	}
}

func TestUploadHandler_MissingSourceName_Returns400(t *testing.T) {
	h := NewUploadHandler()
	req := multipartRequest(t, map[string]string{}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
}

func TestUploadHandler_FileExceedsLimit_Returns413(t *testing.T) {
	h := &UploadHandler{MaxUploadBytes: 10} // 10 bytes, easy to exceed
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, []byte("this payload is definitely more than 10 bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
}

func TestUploadHandler_InvalidRenditionJSON_Returns400(t *testing.T) {
	h := NewUploadHandler()
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  "{not valid json",
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.ErrorCode != "VALIDATION_ERROR" {
		t.Errorf("error_code = %q, want VALIDATION_ERROR", resp.ErrorCode)
	}
}

func TestUploadHandler_MissingVideoFile_Returns400(t *testing.T) {
	h := NewUploadHandler()
	req := multipartRequest(t, map[string]string{"source_name": "clip.mp4"}, nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestUploadHandler_ValidRenditionsJSON_Accepted(t *testing.T) {
	h := NewUploadHandler()
	renditions := `[{"id":"1080p","codec":"libx264","bitrate_kbps":8000,"width":1920,"height":1080}]`
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  renditions,
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestUploadHandler_InvalidRenditionSchema_Returns400(t *testing.T) {
	h := NewUploadHandler()
	// missing required codec/bitrate/width/height for a non-HLS rendition
	renditions := `[{"id":"1080p"}]`
	req := multipartRequest(t, map[string]string{
		"source_name": "clip.mp4",
		"renditions":  renditions,
	}, []byte("fake video bytes"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestUploadHandler_WrongMethod_Returns405(t *testing.T) {
	h := NewUploadHandler()
	req := httptest.NewRequest(http.MethodGet, "/videos/upload", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
