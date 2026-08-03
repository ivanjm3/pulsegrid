package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"pulsegrid/pkg"
)

// maxDownloadAttempts and the backoff schedule match the design doc's retry
// policy used elsewhere for S3 operations: 1s, 2s, 4s, 8s, 16s (max 5
// attempts).
const maxDownloadAttempts = 5

var downloadBackoffSchedule = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// GetObjectAPIClient is the subset of *s3.Client used to download source
// videos, allowing tests to substitute a fake.
type GetObjectAPIClient interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Downloader stages source videos from S3 onto local disk ahead of
// transcoding.
type Downloader struct {
	api GetObjectAPIClient

	// sleep is overridable in tests to avoid real waits during backoff.
	sleep func(time.Duration)
}

// NewDownloader returns a Downloader that fetches objects via api.
func NewDownloader(api GetObjectAPIClient) *Downloader {
	return &Downloader{api: api, sleep: time.Sleep}
}

// parseS3URI splits an "s3://bucket/key" URI into its bucket and key parts.
func parseS3URI(uri string) (bucket, key string, err error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return "", "", fmt.Errorf("invalid s3 uri %q", uri)
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

// DownloadSourceFromS3 downloads the source video at s3URI to
// /tmp/{jobID}/original.mp4, streaming directly to disk. Network errors are
// retried with exponential backoff; a 404 (source object missing) is a
// permanent failure and is returned immediately without retrying. Running
// out of disk space while writing returns a *pkg.ResourceConstraintError.
func (d *Downloader) DownloadSourceFromS3(ctx context.Context, jobID, s3URI string) (string, error) {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return "", fmt.Errorf("download source from s3: %w", err)
	}

	destDir := filepath.Join(os.TempDir(), jobID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", fmt.Errorf("download source from s3: create staging dir: %w", err)
	}
	destPath := filepath.Join(destDir, "original.mp4")

	var lastErr error
	for attempt := 0; attempt < maxDownloadAttempts; attempt++ {
		if attempt > 0 {
			delay := downloadBackoffSchedule[attempt-1]
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
			}
			d.sleep(delay)
		}

		size, err := d.downloadOnce(ctx, bucket, key, destPath)
		if err == nil {
			log.Printf("event=source_download_complete job_id=%s size_bytes=%d attempts=%d", jobID, size, attempt+1)
			return destPath, nil
		}

		if isNotFoundError(err) {
			return "", fmt.Errorf("download source from s3: source object not found: %w", err)
		}

		var rcErr *pkg.ResourceConstraintError
		if errors.As(err, &rcErr) {
			return "", err
		}

		lastErr = err
	}

	return "", fmt.Errorf("download source from s3: exhausted %d attempts: %w", maxDownloadAttempts, lastErr)
}

// downloadOnce performs a single GetObject call and streams the body to
// destPath, returning the number of bytes written.
func (d *Downloader) downloadOnce(ctx context.Context, bucket, key, destPath string) (int64, error) {
	out, err := d.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()

	f, err := os.Create(destPath)
	if err != nil {
		return 0, fmt.Errorf("create destination file: %w", err)
	}
	defer f.Close()

	size, err := io.Copy(f, out.Body)
	if err != nil {
		if isNoSpaceError(err) {
			return size, &pkg.ResourceConstraintError{Resource: "disk", Err: err}
		}
		return size, fmt.Errorf("write source to disk: %w", err)
	}
	return size, nil
}

// isNotFoundError reports whether err is an S3 "object not found" error
// (NoSuchKey / NotFound), which is a permanent, non-retryable failure.
func isNotFoundError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound"
	}
	return false
}

// isNoSpaceError reports whether err indicates the local filesystem ran out
// of space while staging the download.
func isNoSpaceError(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || strings.Contains(err.Error(), "no space left on device")
}
