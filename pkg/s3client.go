package pkg

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const sourceBucket = "pulsegrid-source"

// S3Uploader is the interface for uploading source video to S3.
// Allows mocking in tests and nil-check for local dev.
type S3Uploader interface {
	UploadSourceToS3(ctx context.Context, file io.Reader, jobID string, sourceName string) (string, error)
}

// S3Client wraps AWS SDK v2 S3 client for source uploads.
type S3Client struct {
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
