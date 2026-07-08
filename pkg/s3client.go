package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const sourceBucket = "pulsegrid-source"
const outputBucket = "pulsegrid-output"

// S3Uploader is the interface for uploading source video to S3.
// Allows mocking in tests and nil-check for local dev.
type S3Uploader interface {
	UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error)
	Ping(ctx context.Context) error
}

// S3Client wraps AWS SDK v2 S3 client for source uploads.
type S3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// NewS3Client creates S3Client with multipart upload manager.
func NewS3Client(cfg aws.Config, bucket string) *S3Client {
	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB parts
		u.Concurrency = 5
	})
	return &S3Client{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
	}
}

// UploadSourceToS3 streams file to S3 using multipart upload (no local disk).
// Key: {jobID}/original.mp4
// Tags: job_id, upload_time, source_name
// Returns s3://{bucket}/{key} on success.
func (c *S3Client) UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error) {
	key := fmt.Sprintf("%s/original.mp4", jobID)
	uploadTime := time.Now().UTC().Format(time.RFC3339)

	tagging := fmt.Sprintf("job_id=%s&upload_time=%s&source_name=%s",
		url.QueryEscape(jobID),
		url.QueryEscape(uploadTime),
		url.QueryEscape(sourceName),
	)

	var s3URI string
	err := RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		_, uploadErr := c.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(c.bucket),
			Key:     aws.String(key),
			Body:    file,
			Tagging: aws.String(tagging),
			StorageClass: types.StorageClassStandard,
		})
		if uploadErr != nil {
			return uploadErr
		}
		s3URI = fmt.Sprintf("s3://%s/%s", c.bucket, key)
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("s3 upload failed for job %s: %w", jobID, err)
	}

	return s3URI, nil
}

// Ping checks S3 bucket connectivity using HeadBucket.
func (c *S3Client) Ping(ctx context.Context) error {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		return fmt.Errorf("s3 ping: %w", err)
	}
	return nil
}

// S3Downloader is the interface for downloading source video from S3.
// Allows mocking in tests and nil-check for local dev.
type S3Downloader interface {
	DownloadSourceFromS3(ctx context.Context, jobID string, s3URI string) (string, error)
}

// S3OutputUploader is the interface for uploading transcoded outputs to S3.
// Allows mocking in tests and nil-check for local dev.
type S3OutputUploader interface {
	UploadOutputsToS3(ctx context.Context, jobID string, results []*TranscodeResult, hlsResults []*HLSResult, manifestPath string) error
}

// ParseS3URI extracts bucket and key from an s3:// URI.
// Returns bucket, key, error.
func ParseS3URI(s3URI string) (string, string, error) {
	if !strings.HasPrefix(s3URI, "s3://") {
		return "", "", fmt.Errorf("invalid s3 URI (missing s3:// prefix): %s", s3URI)
	}
	trimmed := strings.TrimPrefix(s3URI, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 URI (missing bucket or key): %s", s3URI)
	}
	return parts[0], parts[1], nil
}

// DownloadSourceFromS3 downloads source video from S3 to local disk.
// Stores file at /tmp/{jobID}/original.mp4.
// Returns local file path on success.
// Returns *SourceNotFoundError for 404 (permanent, no retry).
// Retries transient network errors with exponential backoff.
// Logs download size and elapsed time.
func (c *S3Client) DownloadSourceFromS3(ctx context.Context, jobID string, s3URI string) (string, error) {
	start := time.Now()

	bucket, key, err := ParseS3URI(s3URI)
	if err != nil {
		return "", fmt.Errorf("download source [job=%s]: %w", jobID, err)
	}

	// Create temp directory
	dir := filepath.Join(os.TempDir(), jobID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("download source [job=%s]: mkdir failed: %w", jobID, err)
	}

	localPath := filepath.Join(dir, "original.mp4")

	var downloadSize int64
	var permanentErr error

	err = RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		// Get object from S3
		output, getErr := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if getErr != nil {
			// Check for 404 NoSuchKey — permanent failure, no retry
			var noSuchKey *types.NoSuchKey
			if errors.As(getErr, &noSuchKey) {
				permanentErr = &SourceNotFoundError{
					JobID:   jobID,
					S3URI:   s3URI,
					Message: "object does not exist in S3",
				}
				return nil // stop retrying
			}
			// Also check for NotFound via generic API error
			var nfe *types.NotFound
			if errors.As(getErr, &nfe) {
				permanentErr = &SourceNotFoundError{
					JobID:   jobID,
					S3URI:   s3URI,
					Message: "object not found in S3",
				}
				return nil // stop retrying
			}
			// Transient — retry
			return getErr
		}
		defer output.Body.Close()

		// Stream to disk
		f, createErr := os.Create(localPath)
		if createErr != nil {
			return fmt.Errorf("create file: %w", createErr)
		}
		defer f.Close()

		written, copyErr := io.Copy(f, output.Body)
		if copyErr != nil {
			// Clean up partial file
			os.Remove(localPath)
			return copyErr
		}

		downloadSize = written
		return nil
	})

	if permanentErr != nil {
		return "", permanentErr
	}
	if err != nil {
		return "", fmt.Errorf("download source [job=%s] from %s failed: %w", jobID, s3URI, err)
	}

	elapsed := time.Since(start)
	log.Printf("s3 download complete: job=%s size=%d bytes duration=%v uri=%s",
		jobID, downloadSize, elapsed, s3URI)

	return localPath, nil
}

// isPermanentS3Error checks if an error is permanent (403 AccessDenied) and should not be retried.
func isPermanentS3Error(err error) bool {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if respErr.Response != nil && respErr.Response.Response != nil {
			code := respErr.Response.Response.StatusCode
			// 403 = AccessDenied, 404 = NotFound — both permanent
			return code == http.StatusForbidden || code == http.StatusNotFound
		}
	}
	return false
}

// uploadSingleFile uploads one file to S3 output bucket with tags and retry.
// Returns error immediately (no retry) for permanent errors (403).
func (c *S3Client) uploadSingleFile(ctx context.Context, localPath string, key string, tagging string) error {
	var permanentErr error

	err := RetryWithBackoff(ctx, 5, 1*time.Second, func() error {
		f, openErr := os.Open(localPath)
		if openErr != nil {
			// Can't open local file — permanent, no retry
			permanentErr = fmt.Errorf("cannot open file %s: %w", localPath, openErr)
			return nil
		}
		defer f.Close()

		_, uploadErr := c.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:       aws.String(outputBucket),
			Key:          aws.String(key),
			Body:         f,
			Tagging:      aws.String(tagging),
			StorageClass: types.StorageClassStandard,
		})
		if uploadErr != nil {
			if isPermanentS3Error(uploadErr) {
				permanentErr = uploadErr
				return nil // stop retrying
			}
			return uploadErr // transient, retry
		}
		return nil
	})

	if permanentErr != nil {
		return fmt.Errorf("permanent s3 error uploading %s: %w", key, permanentErr)
	}
	if err != nil {
		return fmt.Errorf("s3 output upload failed for %s: %w", key, err)
	}
	return nil
}

// UploadOutputsToS3 uploads all transcoded output files and manifest to the output S3 bucket.
// Path structure: s3://pulsegrid-output/{jobID}/{rendition}/{filename}
// Manifest at: s3://pulsegrid-output/{jobID}/manifest.json
// Tags: job_id, completion_time, rendition
// Retries transient errors with exponential backoff. Returns immediately on permanent errors (403).
func (c *S3Client) UploadOutputsToS3(ctx context.Context, jobID string, results []*TranscodeResult, hlsResults []*HLSResult, manifestPath string) error {
	start := time.Now()
	completionTime := time.Now().UTC().Format(time.RFC3339)

	// Upload MP4 rendition files
	for _, r := range results {
		filename := filepath.Base(r.FilePath)
		key := fmt.Sprintf("%s/%s/%s", jobID, r.RenditionID, filename)

		tagging := fmt.Sprintf("job_id=%s&completion_time=%s&rendition=%s",
			url.QueryEscape(jobID),
			url.QueryEscape(completionTime),
			url.QueryEscape(r.RenditionID),
		)

		if err := c.uploadSingleFile(ctx, r.FilePath, key, tagging); err != nil {
			return fmt.Errorf("upload output [job=%s rendition=%s]: %w", jobID, r.RenditionID, err)
		}

		log.Printf("s3 output uploaded: job=%s rendition=%s key=%s size=%d",
			jobID, r.RenditionID, key, r.FileSize)
	}

	// Upload HLS rendition files (playlist + segments)
	for _, h := range hlsResults {
		// Upload playlist
		playlistFilename := filepath.Base(h.PlaylistPath)
		playlistKey := fmt.Sprintf("%s/%s/%s", jobID, h.RenditionID, playlistFilename)

		tagging := fmt.Sprintf("job_id=%s&completion_time=%s&rendition=%s",
			url.QueryEscape(jobID),
			url.QueryEscape(completionTime),
			url.QueryEscape(h.RenditionID),
		)

		if err := c.uploadSingleFile(ctx, h.PlaylistPath, playlistKey, tagging); err != nil {
			return fmt.Errorf("upload hls playlist [job=%s rendition=%s]: %w", jobID, h.RenditionID, err)
		}

		// Upload each segment
		for _, segPath := range h.Segments {
			segFilename := filepath.Base(segPath)
			segKey := fmt.Sprintf("%s/%s/%s", jobID, h.RenditionID, segFilename)

			if err := c.uploadSingleFile(ctx, segPath, segKey, tagging); err != nil {
				return fmt.Errorf("upload hls segment [job=%s rendition=%s seg=%s]: %w", jobID, h.RenditionID, segFilename, err)
			}
		}

		log.Printf("s3 hls uploaded: job=%s rendition=%s playlist=%s segments=%d",
			jobID, h.RenditionID, playlistKey, len(h.Segments))
	}

	// Upload manifest
	manifestKey := fmt.Sprintf("%s/manifest.json", jobID)
	manifestTagging := fmt.Sprintf("job_id=%s&completion_time=%s&rendition=%s",
		url.QueryEscape(jobID),
		url.QueryEscape(completionTime),
		url.QueryEscape("manifest"),
	)

	if err := c.uploadSingleFile(ctx, manifestPath, manifestKey, manifestTagging); err != nil {
		return fmt.Errorf("upload manifest [job=%s]: %w", jobID, err)
	}

	elapsed := time.Since(start)
	totalFiles := len(results) + len(hlsResults) + 1 // +1 for manifest
	for _, h := range hlsResults {
		totalFiles += len(h.Segments) // segments
	}

	log.Printf("s3 output upload complete: job=%s files=%d duration=%v", jobID, totalFiles, elapsed)

	return nil
}
