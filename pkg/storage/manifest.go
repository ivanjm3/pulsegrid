package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"pulsegrid/pkg"
)

// ErrManifestNotFound is returned by FetchManifest when the job's
// manifest.json does not exist in the output bucket (e.g. the worker hasn't
// finished uploading outputs yet).
var ErrManifestNotFound = errors.New("manifest not found")

// GetObjectAPIClient is the subset of *s3.Client used to fetch manifests,
// allowing tests to substitute a fake.
type GetObjectAPIClient interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Downloader fetches job manifests from the output S3 bucket.
type Downloader struct {
	api    GetObjectAPIClient
	bucket string
}

// NewDownloader returns a Downloader that reads manifests from bucket via api.
func NewDownloader(api GetObjectAPIClient, bucket string) *Downloader {
	return &Downloader{api: api, bucket: bucket}
}

// FetchManifest downloads and parses s3://{bucket}/{jobID}/manifest.json.
func (d *Downloader) FetchManifest(ctx context.Context, jobID string) (pkg.Manifest, error) {
	key := fmt.Sprintf("%s/manifest.json", jobID)

	out, err := d.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    &key,
	})
	if err != nil {
		if isNoSuchKey(err) {
			return pkg.Manifest{}, ErrManifestNotFound
		}
		return pkg.Manifest{}, fmt.Errorf("fetch manifest for job %s: %w", jobID, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return pkg.Manifest{}, fmt.Errorf("fetch manifest for job %s: read body: %w", jobID, err)
	}

	var manifest pkg.Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return pkg.Manifest{}, fmt.Errorf("fetch manifest for job %s: parse json: %w", jobID, err)
	}
	return manifest, nil
}

func isNoSuchKey(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "NoSuchKey"
	}
	return false
}
