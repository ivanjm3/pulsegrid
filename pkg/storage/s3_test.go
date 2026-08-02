package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// fakeS3Client implements manager.UploadAPIClient. putObjectFn drives
// PutObject behavior per call; other methods are unused because test bodies
// are small enough for the uploader to complete in a single PutObject.
type fakeS3Client struct {
	putObjectFn func(callNum int) (*s3.PutObjectOutput, error)
	calls       int
	lastInput   *s3.PutObjectInput
}

func (f *fakeS3Client) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.calls++
	f.lastInput = in
	return f.putObjectFn(f.calls)
}

func (f *fakeS3Client) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, errors.New("UploadPart not expected in these tests")
}

func (f *fakeS3Client) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, errors.New("CreateMultipartUpload not expected in these tests")
}

func (f *fakeS3Client) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, errors.New("CompleteMultipartUpload not expected in these tests")
}

func (f *fakeS3Client) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, errors.New("AbortMultipartUpload not expected in these tests")
}

type transientAPIError struct{ code string }

func (e transientAPIError) Error() string                 { return "transient: " + e.code }
func (e transientAPIError) ErrorCode() string             { return e.code }
func (e transientAPIError) ErrorMessage() string          { return e.Error() }
func (e transientAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestUploadSource_Success_TaggedCorrectly(t *testing.T) {
	fake := &fakeS3Client{
		putObjectFn: func(callNum int) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
	}
	u := NewUploader(fake, "pulsegrid-source")
	u.sleep = func(time.Duration) {}

	body := bytes.NewReader([]byte("fake video bytes"))
	uri, err := u.UploadSource(context.Background(), "job-123", "marketing.mp4", body)
	if err != nil {
		t.Fatalf("UploadSource returned error: %v", err)
	}
	if want := "s3://pulsegrid-source/job-123/original.mp4"; uri != want {
		t.Fatalf("uri = %q, want %q", uri, want)
	}
	if fake.calls != 1 {
		t.Fatalf("PutObject called %d times, want 1", fake.calls)
	}

	if fake.lastInput.Key == nil || *fake.lastInput.Key != "job-123/original.mp4" {
		t.Fatalf("unexpected key: %v", fake.lastInput.Key)
	}
	if fake.lastInput.Tagging == nil {
		t.Fatal("Tagging not set")
	}
	tagging := *fake.lastInput.Tagging
	for _, want := range []string{"job_id=job-123", "source_name=marketing.mp4", "upload_time="} {
		if !strings.Contains(tagging, want) {
			t.Errorf("tagging %q missing %q", tagging, want)
		}
	}
}

func TestUploadSource_TransientError_RetriesWithBackoff(t *testing.T) {
	fake := &fakeS3Client{
		putObjectFn: func(callNum int) (*s3.PutObjectOutput, error) {
			if callNum < 3 {
				return nil, transientAPIError{code: "SlowDown"}
			}
			return &s3.PutObjectOutput{}, nil
		},
	}
	u := NewUploader(fake, "pulsegrid-source")

	var sleeps []time.Duration
	u.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }

	body := bytes.NewReader([]byte("fake video bytes"))
	uri, err := u.UploadSource(context.Background(), "job-456", "clip.mp4", body)
	if err != nil {
		t.Fatalf("UploadSource returned error: %v", err)
	}
	if uri == "" {
		t.Fatal("expected non-empty uri after eventual success")
	}
	if fake.calls != 3 {
		t.Fatalf("PutObject called %d times, want 3", fake.calls)
	}

	wantSleeps := []time.Duration{1 * time.Second, 2 * time.Second}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleeps = %v, want %v", sleeps, wantSleeps)
	}
	for i, d := range wantSleeps {
		if sleeps[i] != d {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], d)
		}
	}
}

func TestUploadSource_PermanentError_NoRetry(t *testing.T) {
	fake := &fakeS3Client{
		putObjectFn: func(callNum int) (*s3.PutObjectOutput, error) {
			return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
		},
	}
	u := NewUploader(fake, "pulsegrid-source")
	u.sleep = func(time.Duration) { t.Fatal("sleep should not be called for a permanent error") }

	body := bytes.NewReader([]byte("fake video bytes"))
	_, err := u.UploadSource(context.Background(), "job-789", "clip.mp4", body)
	if err == nil {
		t.Fatal("expected error for AccessDenied")
	}
	if fake.calls != 1 {
		t.Fatalf("PutObject called %d times, want 1 (no retry on permanent error)", fake.calls)
	}
}

func TestUploadSource_ExhaustsRetries_ReturnsError(t *testing.T) {
	fake := &fakeS3Client{
		putObjectFn: func(callNum int) (*s3.PutObjectOutput, error) {
			return nil, transientAPIError{code: "SlowDown"}
		},
	}
	u := NewUploader(fake, "pulsegrid-source")
	u.sleep = func(time.Duration) {}

	body := bytes.NewReader([]byte("fake video bytes"))
	_, err := u.UploadSource(context.Background(), "job-999", "clip.mp4", body)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if fake.calls != maxUploadAttempts {
		t.Fatalf("PutObject called %d times, want %d", fake.calls, maxUploadAttempts)
	}
}
