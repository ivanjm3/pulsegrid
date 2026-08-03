package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// fakeOutputS3Client implements OutputAPIClient. putObjectFn drives
// PutObject behavior per call, keyed by the object key being uploaded.
type fakeOutputS3Client struct {
	putObjectFn func(key string, callNum int) (*s3.PutObjectOutput, error)
	calls       map[string]int
	puts        []*s3.PutObjectInput
	deletes     []string
}

func newFakeOutputS3Client() *fakeOutputS3Client {
	return &fakeOutputS3Client{calls: map[string]int{}}
}

func (f *fakeOutputS3Client) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := *in.Key
	f.calls[key]++
	f.puts = append(f.puts, in)
	return f.putObjectFn(key, f.calls[key])
}

func (f *fakeOutputS3Client) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.deletes = append(f.deletes, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeOutputS3Client) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	panic("UploadPart not expected in these tests")
}

func (f *fakeOutputS3Client) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	panic("CreateMultipartUpload not expected in these tests")
}

func (f *fakeOutputS3Client) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	panic("CompleteMultipartUpload not expected in these tests")
}

func (f *fakeOutputS3Client) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	panic("AbortMultipartUpload not expected in these tests")
}

func writeTempFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func TestUploadOutputs_Success_TaggedCorrectly(t *testing.T) {
	dir := t.TempDir()
	renditionPath := writeTempFile(t, dir, "720p.mp4", "fake mp4 bytes")
	manifestPath := writeTempFile(t, dir, "manifest.json", `{"job_id":"job-1"}`)

	fake := newFakeOutputS3Client()
	fake.putObjectFn = func(key string, callNum int) (*s3.PutObjectOutput, error) {
		return &s3.PutObjectOutput{}, nil
	}

	u := NewOutputUploader(fake, "pulsegrid-output")
	u.sleep = func(time.Duration) {}

	files := []OutputFile{{LocalPath: renditionPath, Rendition: "720p", Key: "720p/720p.mp4"}}
	if err := u.UploadOutputs(context.Background(), "job-1", files, manifestPath); err != nil {
		t.Fatalf("UploadOutputs: %v", err)
	}

	if len(fake.puts) != 2 {
		t.Fatalf("PutObject called %d times, want 2 (rendition + manifest)", len(fake.puts))
	}

	renditionPut := fake.puts[0]
	if *renditionPut.Key != "job-1/720p/720p.mp4" {
		t.Errorf("rendition key = %q", *renditionPut.Key)
	}
	tagging := *renditionPut.Tagging
	for _, want := range []string{"job_id=job-1", "rendition=720p", "completion_time="} {
		if !strings.Contains(tagging, want) {
			t.Errorf("tagging %q missing %q", tagging, want)
		}
	}

	manifestPut := fake.puts[1]
	if *manifestPut.Key != "job-1/manifest.json" {
		t.Errorf("manifest key = %q", *manifestPut.Key)
	}
	if !strings.Contains(*manifestPut.Tagging, "rendition=manifest") {
		t.Errorf("manifest tagging %q missing rendition=manifest", *manifestPut.Tagging)
	}
}

func TestUploadOutputs_TransientError_RetriesThenSucceeds(t *testing.T) {
	dir := t.TempDir()
	renditionPath := writeTempFile(t, dir, "720p.mp4", "fake mp4 bytes")
	manifestPath := writeTempFile(t, dir, "manifest.json", `{}`)

	fake := newFakeOutputS3Client()
	fake.putObjectFn = func(key string, callNum int) (*s3.PutObjectOutput, error) {
		if key == "job-2/720p/720p.mp4" && callNum < 3 {
			return nil, &smithy.GenericAPIError{Code: "SlowDown", Message: "slow down", Fault: smithy.FaultServer}
		}
		return &s3.PutObjectOutput{}, nil
	}

	u := NewOutputUploader(fake, "pulsegrid-output")
	var sleeps []time.Duration
	u.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }

	files := []OutputFile{{LocalPath: renditionPath, Rendition: "720p", Key: "720p/720p.mp4"}}
	if err := u.UploadOutputs(context.Background(), "job-2", files, manifestPath); err != nil {
		t.Fatalf("UploadOutputs: %v", err)
	}

	if fake.calls["job-2/720p/720p.mp4"] != 3 {
		t.Fatalf("rendition uploaded %d times, want 3", fake.calls["job-2/720p/720p.mp4"])
	}
	if len(sleeps) != 2 {
		t.Fatalf("slept %d times, want 2", len(sleeps))
	}
}

func TestUploadOutputs_PermanentError_NoRetry(t *testing.T) {
	dir := t.TempDir()
	renditionPath := writeTempFile(t, dir, "720p.mp4", "fake mp4 bytes")
	manifestPath := writeTempFile(t, dir, "manifest.json", `{}`)

	fake := newFakeOutputS3Client()
	fake.putObjectFn = func(key string, callNum int) (*s3.PutObjectOutput, error) {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "forbidden"}
	}

	u := NewOutputUploader(fake, "pulsegrid-output")
	u.sleep = func(time.Duration) { t.Fatal("sleep should not be called for a permanent error") }

	files := []OutputFile{{LocalPath: renditionPath, Rendition: "720p", Key: "720p/720p.mp4"}}
	err := u.UploadOutputs(context.Background(), "job-3", files, manifestPath)
	if err == nil {
		t.Fatal("expected error for AccessDenied")
	}
	if fake.calls["job-3/720p/720p.mp4"] != 1 {
		t.Fatalf("rendition uploaded %d times, want 1 (no retry)", fake.calls["job-3/720p/720p.mp4"])
	}
}

func TestUploadOutputs_PartialFailure_CleansUpUploaded(t *testing.T) {
	dir := t.TempDir()
	p720 := writeTempFile(t, dir, "720p.mp4", "fake 720p bytes")
	p480 := writeTempFile(t, dir, "480p.mp4", "fake 480p bytes")
	manifestPath := writeTempFile(t, dir, "manifest.json", `{}`)

	fake := newFakeOutputS3Client()
	fake.putObjectFn = func(key string, callNum int) (*s3.PutObjectOutput, error) {
		if key == "job-4/480p/480p.mp4" {
			return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "forbidden"}
		}
		return &s3.PutObjectOutput{}, nil
	}

	u := NewOutputUploader(fake, "pulsegrid-output")
	u.sleep = func(time.Duration) {}

	files := []OutputFile{
		{LocalPath: p720, Rendition: "720p", Key: "720p/720p.mp4"},
		{LocalPath: p480, Rendition: "480p", Key: "480p/480p.mp4"},
	}
	err := u.UploadOutputs(context.Background(), "job-4", files, manifestPath)
	if err == nil {
		t.Fatal("expected error when one file fails to upload")
	}

	if len(fake.deletes) != 1 || fake.deletes[0] != "job-4/720p/720p.mp4" {
		t.Fatalf("deletes = %v, want cleanup of job-4/720p/720p.mp4", fake.deletes)
	}
	// The manifest is uploaded last, so it should never have been attempted.
	if fake.calls["job-4/manifest.json"] != 0 {
		t.Fatalf("manifest uploaded %d times, want 0 (job failed before manifest step)", fake.calls["job-4/manifest.json"])
	}
}
