package main

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/storage"
	"pulsegrid/pkg/worker"
)

// writeFakeFFmpeg writes an executable shell script standing in for the real
// ffmpeg binary: it always emits a Duration line to stderr, then either
// produces a single fake output file, or (when invoked with "-f hls") a
// playlist plus two fake .ts segments.
func writeFakeFFmpeg(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "ffmpeg")
	script := `#!/bin/sh
echo "Duration: 00:00:05.00, start: 0.000000, bitrate: 500 kb/s" >&2
case "$*" in
  *"-f hls"*)
    eval out=\${$#}
    outdir=$(dirname "$out")
    echo "#EXTM3U" > "$out"
    echo "fake ts data" > "$outdir/segment-00000.ts"
    echo "fake ts data" > "$outdir/segment-00001.ts"
    ;;
  *)
    eval out=\${$#}
    echo "fake mp4 bytes" > "$out"
    ;;
esac
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

// fakeGetObjectClient implements worker.GetObjectAPIClient, always returning
// fake source video bytes.
type fakeGetObjectClient struct{}

func (fakeGetObjectClient) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("fake source video bytes"))}, nil
}

// recordedPut snapshots a PutObject call: the caller's *os.File body is
// closed right after Upload returns (see storage.OutputUploader.uploadFile),
// so the body must be read eagerly here rather than kept as a live handle.
type recordedPut struct {
	key     string
	tagging string
	body    []byte
}

// fakeOutputS3Client implements storage.OutputAPIClient, recording every
// PutObject call (key, tagging, body) so the test can assert on S3 paths,
// tags, and uploaded content.
type fakeOutputS3Client struct {
	puts []recordedPut
}

func (f *fakeOutputS3Client) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	tagging := ""
	if in.Tagging != nil {
		tagging = *in.Tagging
	}
	f.puts = append(f.puts, recordedPut{key: *in.Key, tagging: tagging, body: body})
	return &s3.PutObjectOutput{}, nil
}
func (f *fakeOutputS3Client) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}
func (f *fakeOutputS3Client) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	panic("UploadPart not expected: fake files are small enough for a single PutObject")
}
func (f *fakeOutputS3Client) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	panic("CreateMultipartUpload not expected")
}
func (f *fakeOutputS3Client) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	panic("CompleteMultipartUpload not expected")
}
func (f *fakeOutputS3Client) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	panic("AbortMultipartUpload not expected")
}

// fakeRetryPublisher and fakeDLQPublisher stand in for the retry/DLQ Kafka
// producers; the happy-path test never uses them, but LifecycleHandler
// requires the interfaces to be satisfied.
type fakeRetryPublisher struct{}

func (fakeRetryPublisher) EnqueueJob(ctx context.Context, job pkg.Job) error { return nil }

type fakeDLQPublisher struct{}

func (fakeDLQPublisher) SendDLQ(ctx context.Context, msg queue.DLQMessage) error { return nil }

// fakeStore records every status event and jobs-table transition recorded
// during job processing.
type fakeStore struct {
	events []string
}

func (f *fakeStore) RecordStatusEvent(ctx context.Context, jobID, eventType string, eventData map[string]any, podID string) error {
	f.events = append(f.events, eventType)
	return nil
}

func (f *fakeStore) MarkJobProcessing(ctx context.Context, jobID string) error { return nil }
func (f *fakeStore) MarkJobCompleted(ctx context.Context, jobID string) error  { return nil }
func (f *fakeStore) MarkJobFailed(ctx context.Context, jobID, failureReason string, retryCount int) error {
	return nil
}

// fakeKafkaReader is a worker.MessageReader backed by a single in-memory
// message; FetchMessage blocks after it's been consumed once, mirroring a
// real reader waiting for more data.
type fakeKafkaReader struct {
	msg       kafka.Message
	delivered bool
	committed []kafka.Message
	closed    bool
}

func (f *fakeKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if !f.delivered {
		f.delivered = true
		return f.msg, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeKafkaReader) Close() error {
	f.closed = true
	return nil
}

// TestWorkerPod_EndToEnd is the task 22 checkpoint: a full mocked run of the
// worker pod's job lifecycle — consume a Kafka message, download the source
// from S3, transcode a single-file and an HLS rendition, upload every
// output, emit metrics, record status events, and commit the offset.
func TestWorkerPod_EndToEnd(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("HOSTNAME", "worker-pod-e2e")

	ffmpegDir := t.TempDir()
	ffmpeg := writeFakeFFmpeg(t, ffmpegDir)

	downloader := worker.NewDownloader(fakeGetObjectClient{})

	transcoder := worker.NewTranscoder()
	transcoder.SetFFmpegPath(ffmpeg)

	outputClient := &fakeOutputS3Client{}
	uploader := storage.NewOutputUploader(outputClient, "pulsegrid-output-test")

	store := &fakeStore{}
	m := metrics.NewWorker()
	logger := worker.NewLogger(io.Discard)
	lifecycle := worker.NewLifecycleHandler(fakeRetryPublisher{}, fakeDLQPublisher{}, store, m, "worker-pod-e2e", logger)

	handler := &jobHandler{
		downloader: downloader,
		transcoder: transcoder,
		uploader:   uploader,
		lifecycle:  lifecycle,
		metrics:    m,
		logger:     logger,
		podID:      "worker-pod-e2e",
	}

	jobID := "e2e-job-1"
	jobMsg := queue.JobMessage{
		JobID:       jobID,
		SourceS3URI: "s3://pulsegrid-source/" + jobID + "/original.mp4",
		Renditions: []pkg.Rendition{
			{ID: "480p", Codec: "libx264", BitrateKbps: 1000, Width: 854, Height: 480},
			{ID: "hls-720p", Codec: "libx264", BitrateKbps: 2500, Width: 1280, Height: 720, HLS: true},
		},
		OutputS3Prefix:     "s3://pulsegrid-output-test/" + jobID,
		SubmittedTimestamp: time.Now().UTC().Format(time.RFC3339),
		RetryCount:         0,
	}
	body, err := json.Marshal(jobMsg)
	if err != nil {
		t.Fatalf("marshal job message: %v", err)
	}

	reader := &fakeKafkaReader{msg: kafka.Message{Partition: 0, Offset: 0, Value: body}}
	consumer := worker.NewConsumer(reader, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	deadline := time.After(9 * time.Second)
	for len(reader.committed) < 1 {
		select {
		case <-deadline:
			t.Fatal("job was never processed and committed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// Consumer committed the offset: successful end-to-end processing.
	if len(reader.committed) != 1 {
		t.Fatalf("committed count = %d, want 1", len(reader.committed))
	}

	// Job start and completion recorded.
	if len(store.events) != 2 || store.events[0] != "job_started" || store.events[1] != "job_completed" {
		t.Fatalf("status events = %v, want [job_started job_completed]", store.events)
	}

	// pulsegrid_job_completed_total incremented.
	if got := promtestutil.ToFloat64(m.JobCompletedTotal); got != 1 {
		t.Fatalf("JobCompletedTotal = %v, want 1", got)
	}

	// Duration histogram observed for both renditions.
	for _, rendition := range []string{"480p", "hls-720p"} {
		count := promtestutil.CollectAndCount(m.TranscodeDurationSeconds)
		if count == 0 {
			t.Fatalf("TranscodeDurationSeconds has no observations (rendition %s)", rendition)
		}
	}

	// Expected S3 keys: {jobID}/480p/480p.mp4, {jobID}/hls-720p/playlist.m3u8,
	// two .ts segments under hls-720p/, and {jobID}/manifest.json.
	gotKeys := map[string]recordedPut{}
	for _, put := range outputClient.puts {
		gotKeys[put.key] = put
	}

	mp4Key := jobID + "/480p/480p.mp4"
	if _, ok := gotKeys[mp4Key]; !ok {
		t.Fatalf("missing uploaded key %q; got keys %v", mp4Key, keysOf(outputClient.puts))
	}
	playlistKey := jobID + "/hls-720p/playlist.m3u8"
	if _, ok := gotKeys[playlistKey]; !ok {
		t.Fatalf("missing uploaded key %q; got keys %v", playlistKey, keysOf(outputClient.puts))
	}
	manifestKey := jobID + "/manifest.json"
	manifestPut, ok := gotKeys[manifestKey]
	if !ok {
		t.Fatalf("missing uploaded key %q; got keys %v", manifestKey, keysOf(outputClient.puts))
	}

	segmentCount := 0
	for key := range gotKeys {
		if strings.HasPrefix(key, jobID+"/hls-720p/segment-") {
			segmentCount++
		}
	}
	if segmentCount != 2 {
		t.Fatalf("hls segment uploads = %d, want 2; got keys %v", segmentCount, keysOf(outputClient.puts))
	}

	// Tagging: every rendition file is tagged job_id + rendition; the
	// manifest is tagged rendition="manifest".
	mp4Put := gotKeys[mp4Key]
	tags, err := url.ParseQuery(mp4Put.tagging)
	if err != nil {
		t.Fatalf("parse tags: %v", err)
	}
	if tags.Get("job_id") != jobID {
		t.Fatalf("mp4 tag job_id = %q, want %q", tags.Get("job_id"), jobID)
	}
	if tags.Get("rendition") != "480p" {
		t.Fatalf("mp4 tag rendition = %q, want 480p", tags.Get("rendition"))
	}
	manifestTags, err := url.ParseQuery(manifestPut.tagging)
	if err != nil {
		t.Fatalf("parse manifest tags: %v", err)
	}
	if manifestTags.Get("rendition") != "manifest" {
		t.Fatalf("manifest tag rendition = %q, want manifest", manifestTags.Get("rendition"))
	}

	// Manifest body is valid JSON and lists both renditions.
	var manifest pkg.Manifest
	if err := json.Unmarshal(manifestPut.body, &manifest); err != nil {
		t.Fatalf("manifest is not valid JSON: %v\nraw: %s", err, manifestPut.body)
	}
	if manifest.JobID != jobID {
		t.Fatalf("manifest job_id = %q, want %q", manifest.JobID, jobID)
	}
	if len(manifest.OutputFiles) != 2 {
		t.Fatalf("manifest output_files = %d, want 2: %+v", len(manifest.OutputFiles), manifest.OutputFiles)
	}

	// Staging directory cleaned up after processing.
	if _, err := os.Stat(filepath.Join(os.TempDir(), jobID)); !os.IsNotExist(err) {
		t.Fatalf("temp dir for %s still exists after processing: err=%v", jobID, err)
	}
}

func keysOf(puts []recordedPut) []string {
	keys := make([]string, len(puts))
	for i, p := range puts {
		keys[i] = p.key
	}
	return keys
}
