// Package storage implements S3 upload of source videos for the API server.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// maxUploadAttempts and the backoff schedule match the design doc's retry
// policy for S3 uploads: 1s, 2s, 4s, 8s, 16s (max 5 attempts).
const maxUploadAttempts = 5

var backoffSchedule = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// permanentErrorCodes are S3 error codes that should never be retried.
var permanentErrorCodes = map[string]bool{
	"AccessDenied":          true,
	"NoSuchBucket":          true,
	"InvalidAccessKeyId":    true,
	"SignatureDoesNotMatch": true,
}

// Uploader uploads source videos to S3 using the AWS SDK v2 multipart upload
// manager, streaming directly from the request body with no local disk
// buffering.
type Uploader struct {
	api    manager.UploadAPIClient
	bucket string

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewUploader returns an Uploader that stores objects in bucket via api.
func NewUploader(api manager.UploadAPIClient, bucket string) *Uploader {
	return &Uploader{api: api, bucket: bucket, sleep: time.Sleep}
}

// UploadSource uploads body to s3://{bucket}/{jobID}/original.mp4, tagging
// the object with job_id, upload_time, and source_name. It retries transient
// failures with exponential backoff and returns immediately on permanent
// errors (e.g. AccessDenied).
//
// body must be an io.ReadSeeker: a failed upload attempt may have partially
// consumed the underlying data, so each retry seeks back to the start before
// resending.
func (u *Uploader) UploadSource(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error) {
	key := fmt.Sprintf("%s/original.mp4", jobID)
	uploadTime := time.Now().UTC().Format(time.RFC3339)

	tags := url.Values{}
	tags.Set("job_id", jobID)
	tags.Set("upload_time", uploadTime)
	tags.Set("source_name", sourceName)
	tagging := tags.Encode()

	uploader := manager.NewUploader(u.api)

	var lastErr error
	for attempt := 0; attempt < maxUploadAttempts; attempt++ {
		if attempt > 0 {
			delay := backoffSchedule[attempt-1]
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
			}
			u.sleep(delay)
		}

		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return "", fmt.Errorf("upload source to s3: rewind body for retry: %w", err)
		}

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:  &u.bucket,
			Key:     &key,
			Body:    body,
			Tagging: &tagging,
		})
		if err == nil {
			return fmt.Sprintf("s3://%s/%s", u.bucket, key), nil
		}

		lastErr = err
		if isPermanentUploadError(err) {
			return "", fmt.Errorf("upload source to s3: %w", err)
		}
	}

	return "", fmt.Errorf("upload source to s3: exhausted %d attempts: %w", maxUploadAttempts, lastErr)
}

// isPermanentUploadError reports whether err represents a non-retryable S3
// failure (e.g. AccessDenied) rather than a transient one.
func isPermanentUploadError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return permanentErrorCodes[apiErr.ErrorCode()]
	}
	return false
}

// HeadBucketAPIClient is the subset of *s3.Client used to check bucket
// reachability, allowing tests to substitute a fake.
type HeadBucketAPIClient interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// BucketPinger checks S3 connectivity for health checks by confirming a
// bucket is reachable.
type BucketPinger struct {
	api    HeadBucketAPIClient
	bucket string
}

// NewBucketPinger returns a BucketPinger that checks bucket via api.
func NewBucketPinger(api HeadBucketAPIClient, bucket string) *BucketPinger {
	return &BucketPinger{api: api, bucket: bucket}
}

// Ping confirms the configured bucket is reachable.
func (b *BucketPinger) Ping(ctx context.Context) error {
	_, err := b.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &b.bucket})
	if err != nil {
		return fmt.Errorf("ping s3 bucket %s: %w", b.bucket, err)
	}
	return nil
}
