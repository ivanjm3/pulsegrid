package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"

	"pulsegrid/pkg"
)

// fakeGetObjectClient implements GetObjectAPIClient. getObjectFn drives
// GetObject behavior per call, letting tests script transient failures
// followed by success.
type fakeGetObjectClient struct {
	getObjectFn func(callNum int) (*s3.GetObjectOutput, error)
	calls       int
	lastBucket  string
	lastKey     string
}

func (f *fakeGetObjectClient) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.calls++
	if in.Bucket != nil {
		f.lastBucket = *in.Bucket
	}
	if in.Key != nil {
		f.lastKey = *in.Key
	}
	return f.getObjectFn(f.calls)
}

type transientAPIError struct{ code string }

func (e transientAPIError) Error() string                 { return "s3 error: " + e.code }
func (e transientAPIError) ErrorCode() string             { return e.code }
func (e transientAPIError) ErrorMessage() string          { return e.Error() }
func (e transientAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func bodyOf(s string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(s))
}

func TestDownloadSourceFromS3_Success(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	fake := &fakeGetObjectClient{
		getObjectFn: func(callNum int) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: bodyOf("fake video bytes")}, nil
		},
	}
	d := NewDownloader(fake)
	d.sleep = func(time.Duration) {}

	jobID := "job-abc"
	path, err := d.DownloadSourceFromS3(context.Background(), jobID, "s3://pulsegrid-source/job-abc/original.mp4")
	if err != nil {
		t.Fatalf("DownloadSourceFromS3 returned error: %v", err)
	}
	if filepath.Base(path) != "original.mp4" {
		t.Fatalf("path = %q, want basename original.mp4", path)
	}
	if filepath.Base(filepath.Dir(path)) != jobID {
		t.Fatalf("path = %q, want parent dir %q", path, jobID)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(data) != "fake video bytes" {
		t.Fatalf("file contents = %q, want %q", data, "fake video bytes")
	}
	if fake.lastBucket != "pulsegrid-source" || fake.lastKey != "job-abc/original.mp4" {
		t.Fatalf("GetObject called with bucket=%q key=%q", fake.lastBucket, fake.lastKey)
	}
}

func TestDownloadSourceFromS3_NetworkErrorThenSuccess(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	fake := &fakeGetObjectClient{
		getObjectFn: func(callNum int) (*s3.GetObjectOutput, error) {
			if callNum < 3 {
				return nil, errors.New("connection reset by peer")
			}
			return &s3.GetObjectOutput{Body: bodyOf("recovered bytes")}, nil
		},
	}
	d := NewDownloader(fake)
	d.sleep = func(time.Duration) {}

	path, err := d.DownloadSourceFromS3(context.Background(), "job-retry", "s3://pulsegrid-source/job-retry/original.mp4")
	if err != nil {
		t.Fatalf("DownloadSourceFromS3 returned error: %v", err)
	}
	if fake.calls != 3 {
		t.Fatalf("GetObject called %d times, want 3", fake.calls)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(data) != "recovered bytes" {
		t.Fatalf("file contents = %q", data)
	}
}

func TestDownloadSourceFromS3_NotFound_PermanentNoRetry(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	fake := &fakeGetObjectClient{
		getObjectFn: func(callNum int) (*s3.GetObjectOutput, error) {
			return nil, transientAPIError{code: "NoSuchKey"}
		},
	}
	d := NewDownloader(fake)
	d.sleep = func(time.Duration) {}

	_, err := d.DownloadSourceFromS3(context.Background(), "job-missing", "s3://pulsegrid-source/job-missing/original.mp4")
	if err == nil {
		t.Fatal("expected error for missing source object")
	}
	if fake.calls != 1 {
		t.Fatalf("GetObject called %d times, want 1 (no retry on 404)", fake.calls)
	}
}

// enospcReader returns syscall.ENOSPC on Read to simulate the local disk
// filling up mid-download.
type enospcReader struct{}

func (enospcReader) Read(p []byte) (int, error) { return 0, syscall.ENOSPC }
func (enospcReader) Close() error               { return nil }

func TestDownloadSourceFromS3_OutOfDiskSpace_ReturnsResourceConstraintError(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	fake := &fakeGetObjectClient{
		getObjectFn: func(callNum int) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: enospcReader{}}, nil
		},
	}
	d := NewDownloader(fake)
	d.sleep = func(time.Duration) {}

	_, err := d.DownloadSourceFromS3(context.Background(), "job-full-disk", "s3://pulsegrid-source/job-full-disk/original.mp4")
	if err == nil {
		t.Fatal("expected error")
	}
	var rcErr *pkg.ResourceConstraintError
	if !errors.As(err, &rcErr) {
		t.Fatalf("error = %v, want *pkg.ResourceConstraintError", err)
	}
	if fake.calls != 1 {
		t.Fatalf("GetObject called %d times, want 1 (resource constraint is not retried)", fake.calls)
	}
}
