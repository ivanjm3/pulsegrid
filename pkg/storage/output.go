package storage

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// maxOutputUploadAttempts and outputBackoffSchedule match the design doc's
// retry policy used elsewhere for S3 operations: 1s, 2s, 4s, 8s, 16s (max 5
// attempts).
const maxOutputUploadAttempts = 5

var outputBackoffSchedule = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// OutputAPIClient is the subset of *s3.Client used to upload job outputs and
// clean up after a partial failure, allowing tests to substitute a fake.
type OutputAPIClient interface {
	manager.UploadAPIClient
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// OutputFile describes a single local file staged for upload as part of a
// completed job's outputs.
type OutputFile struct {
	// LocalPath is the file's location on local disk.
	LocalPath string
	// Rendition is the rendition ID this file belongs to (used for tagging).
	Rendition string
	// Key is the destination key relative to "{jobID}/", e.g.
	// "720p/720p.mp4" or "hls/playlist.m3u8".
	Key string
}

// OutputUploader uploads a completed job's transcoded outputs and manifest
// to the output S3 bucket.
type OutputUploader struct {
	api    OutputAPIClient
	bucket string

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewOutputUploader returns an OutputUploader that stores objects in bucket
// via api.
func NewOutputUploader(api OutputAPIClient, bucket string) *OutputUploader {
	return &OutputUploader{api: api, bucket: bucket, sleep: time.Sleep}
}

// UploadOutputs uploads each file to s3://{bucket}/{jobID}/{file.Key}, then
// uploads the manifest at manifestPath to
// s3://{bucket}/{jobID}/manifest.json. Every object is tagged with job_id,
// completion_time, and rendition ("manifest" for the manifest itself).
// Transient errors are retried with exponential backoff; permanent errors
// (e.g. AccessDenied) fail immediately without retrying. If any upload
// fails, previously uploaded objects for this job are deleted before
// returning an error.
func (u *OutputUploader) UploadOutputs(ctx context.Context, jobID string, files []OutputFile, manifestPath string) error {
	completionTime := time.Now().UTC().Format(time.RFC3339)
	uploaded := make([]string, 0, len(files)+1)

	for _, f := range files {
		key := fmt.Sprintf("%s/%s", jobID, f.Key)
		if err := u.uploadFile(ctx, jobID, key, f.LocalPath, f.Rendition, completionTime); err != nil {
			u.cleanup(ctx, uploaded)
			return fmt.Errorf("upload outputs for job %s: %w", jobID, err)
		}
		uploaded = append(uploaded, key)
	}

	manifestKey := fmt.Sprintf("%s/manifest.json", jobID)
	if err := u.uploadFile(ctx, jobID, manifestKey, manifestPath, "manifest", completionTime); err != nil {
		u.cleanup(ctx, uploaded)
		return fmt.Errorf("upload outputs for job %s: manifest: %w", jobID, err)
	}

	return nil
}

// uploadFile uploads the file at localPath to key, retrying transient
// failures with exponential backoff. Permanent errors return immediately.
func (u *OutputUploader) uploadFile(ctx context.Context, jobID, key, localPath, rendition, completionTime string) error {
	tags := url.Values{}
	tags.Set("job_id", jobID)
	tags.Set("completion_time", completionTime)
	tags.Set("rendition", rendition)
	tagging := tags.Encode()

	uploader := manager.NewUploader(u.api)

	var lastErr error
	for attempt := 0; attempt < maxOutputUploadAttempts; attempt++ {
		if attempt > 0 {
			delay := outputBackoffSchedule[attempt-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			u.sleep(delay)
		}

		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("open %s: %w", localPath, err)
		}

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:  &u.bucket,
			Key:     &key,
			Body:    f,
			Tagging: &tagging,
		})
		f.Close()

		if err == nil {
			return nil
		}

		lastErr = err
		if isPermanentUploadError(err) {
			return fmt.Errorf("upload %s: %w", key, err)
		}
	}

	return fmt.Errorf("upload %s: exhausted %d attempts: %w", key, maxOutputUploadAttempts, lastErr)
}

// cleanup best-effort deletes previously uploaded objects after a partial
// upload failure. Delete errors are logged, not returned, so they don't mask
// the original upload failure.
func (u *OutputUploader) cleanup(ctx context.Context, keys []string) {
	for _, key := range keys {
		k := key
		if _, err := u.api.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &u.bucket, Key: &k}); err != nil {
			log.Printf("event=output_cleanup_failed key=%s error=%v", k, err)
		}
	}
}
