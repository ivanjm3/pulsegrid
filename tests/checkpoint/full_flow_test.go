// Package checkpoint is task 27's full-integration checkpoint: it wires the
// real API upload handler, a real worker consumer/transcode/upload pipeline,
// and the real status query handler together — with S3, Kafka, and Postgres
// all mocked — and drives one job through the entire path: POST
// /videos/upload -> Kafka -> worker download/transcode/upload -> GET
// /jobs/{id}. Every prior checkpoint (task 11, task 22) exercised one side
// of this in isolation; this test is the first to chain them.
package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	kafka "github.com/segmentio/kafka-go"

	"pulsegrid/pkg"
	"pulsegrid/pkg/api"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/storage"
	"pulsegrid/pkg/store"
	"pulsegrid/pkg/worker"
)

// ---- fake Postgres (store.DB), matching pkg/store/postgres_test.go's fakeDB
// shape but written locally: this package can't import that unexported test
// type, and the fixture is small enough not to be worth exporting just for
// this one cross-package test.

type fakeJobRow struct {
	jobID, status, sourceFileName, sourceS3URI, outputS3Prefix string
	sourceFileSizeBytes                                        int64
	requestedRenditions                                        []byte
	submissionTime                                             time.Time
	completionTime                                             *time.Time
	retryCount                                                 int
	failureReason                                              *string
}

type fakeDB struct {
	mu           sync.Mutex
	jobs         map[string]fakeJobRow
	statusEvents []string
}

func newFakeDB() *fakeDB { return &fakeDB{jobs: make(map[string]fakeJobRow)} }

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case strings.Contains(sql, "INSERT INTO jobs"):
		f.jobs[args[0].(string)] = fakeJobRow{
			jobID: args[0].(string), status: args[1].(string), sourceFileName: args[2].(string),
			sourceFileSizeBytes: args[3].(int64), sourceS3URI: args[4].(string), outputS3Prefix: args[5].(string),
			requestedRenditions: args[6].([]byte), submissionTime: args[7].(time.Time), retryCount: args[8].(int),
		}
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "DELETE FROM jobs"):
		delete(f.jobs, args[0].(string))
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "UPDATE jobs SET status") && strings.Contains(sql, "failure_reason"):
		row := f.jobs[args[0].(string)]
		row.status = args[1].(string)
		reason := args[2].(string)
		row.failureReason = &reason
		row.retryCount = args[3].(int)
		ct := args[4].(time.Time)
		row.completionTime = &ct
		f.jobs[args[0].(string)] = row
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "UPDATE jobs SET status") && strings.Contains(sql, "completion_time"):
		row := f.jobs[args[0].(string)]
		row.status = args[1].(string)
		ct := args[2].(time.Time)
		row.completionTime = &ct
		f.jobs[args[0].(string)] = row
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "UPDATE jobs SET status"):
		row := f.jobs[args[0].(string)]
		row.status = args[1].(string)
		f.jobs[args[0].(string)] = row
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "INSERT INTO job_status_events"):
		f.statusEvents = append(f.statusEvents, args[1].(string))
		return pgconn.CommandTag{}, nil
	}
	return pgconn.CommandTag{}, errors.New("fakeDB: unrecognized statement: " + sql)
}

type fakeRow struct {
	row fakeJobRow
	ok  bool
}

func (r *fakeRow) Scan(dest ...any) error {
	if !r.ok {
		return pgx.ErrNoRows
	}
	values := []any{
		r.row.jobID, r.row.status, r.row.sourceFileName, r.row.sourceFileSizeBytes,
		r.row.sourceS3URI, r.row.outputS3Prefix, r.row.requestedRenditions,
		r.row.submissionTime, r.row.completionTime, r.row.retryCount, r.row.failureReason,
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = values[i].(string)
		case *int64:
			*p = values[i].(int64)
		case *int:
			*p = values[i].(int)
		case *[]byte:
			*p = values[i].([]byte)
		case *time.Time:
			*p = values[i].(time.Time)
		case **time.Time:
			*p = values[i].(*time.Time)
		case **string:
			*p = values[i].(*string)
		}
	}
	return nil
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.jobs[args[0].(string)]
	return &fakeRow{row: row, ok: ok}
}

func (f *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, errors.New("fakeDB: Query not used by this checkpoint")
}

// ---- fake S3 (source download side): worker.GetObjectAPIClient, always
// returns fixed bytes regardless of key, matching task 22's checkpoint fake.

type fakeSourceS3Client struct{}

func (fakeSourceS3Client) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("fake source video bytes"))}, nil
}

// ---- fake S3 (output side): storage.OutputAPIClient (worker's uploads) and
// storage.GetObjectAPIClient (API's manifest fetch for GET /jobs/{id}),
// backed by one in-memory object map so both sides see the same objects.

type fakeOutputS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeOutputS3() *fakeOutputS3 { return &fakeOutputS3{objects: make(map[string][]byte)} }

func (f *fakeOutputS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[*in.Key] = body
	f.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}
func (f *fakeOutputS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}
func (f *fakeOutputS3) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	panic("UploadPart not expected: fake files are small enough for a single PutObject")
}
func (f *fakeOutputS3) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	panic("CreateMultipartUpload not expected")
}
func (f *fakeOutputS3) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	panic("CompleteMultipartUpload not expected")
}
func (f *fakeOutputS3) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	panic("AbortMultipartUpload not expected")
}
func (f *fakeOutputS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	body, ok := f.objects[*in.Key]
	f.mu.Unlock()
	if !ok {
		return nil, &notFoundError{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

type notFoundError struct{}

func (notFoundError) Error() string           { return "NoSuchKey: not found" }
func (notFoundError) ErrorCode() string       { return "NoSuchKey" }
func (notFoundError) ErrorMessage() string    { return "not found" }
func (notFoundError) ErrorFault() smithyFault { return 0 }

// smithyFault mirrors smithy.ErrorFault's underlying type without importing
// the package just for this zero value.
type smithyFault int

// ---- fake source uploader: api.SourceUploader. Content doesn't need to
// round-trip anywhere (the worker's download side is a fixed fake, same as
// task 22's checkpoint), so this only needs to hand back a plausible URI.

type fakeSourceUploader struct{}

func (fakeSourceUploader) UploadSource(ctx context.Context, jobID, sourceName string, body io.ReadSeeker) (string, error) {
	return "s3://pulsegrid-source-test/" + jobID + "/original.mp4", nil
}

// ---- fake Kafka bridge: real queue.Producer publishes into it (queue.Writer
// side), the worker's real consumer reads from it (worker.MessageReader
// side) — the same message, produced by the real production code on both
// ends, not hand-built by the test.

type fakeBroker struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	delivered int
	committed []kafka.Message
}

func (b *fakeBroker) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range msgs {
		m.Offset = int64(len(b.msgs))
		b.msgs = append(b.msgs, m)
	}
	return nil
}
func (b *fakeBroker) Close() error { return nil }

func (b *fakeBroker) FetchMessage(ctx context.Context) (kafka.Message, error) {
	b.mu.Lock()
	if b.delivered < len(b.msgs) {
		m := b.msgs[b.delivered]
		b.delivered++
		b.mu.Unlock()
		return m, nil
	}
	b.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (b *fakeBroker) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.committed = append(b.committed, msgs...)
	return nil
}

func (b *fakeBroker) committedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.committed)
}

// ---- no-op retry/DLQ publishers: this checkpoint's job always succeeds, so
// neither is ever called.

type noopRetryPublisher struct{}

func (noopRetryPublisher) EnqueueJob(ctx context.Context, job pkg.Job) error {
	return errors.New("unexpected: retry enqueue on the happy path")
}

type noopDLQPublisher struct{}

func (noopDLQPublisher) SendDLQ(ctx context.Context, msg queue.DLQMessage) error {
	return errors.New("unexpected: DLQ send on the happy path")
}

// writeFakeFFmpeg mirrors cmd/worker/main_test.go's fixture: a shell script
// standing in for ffmpeg so this test doesn't need the real binary.
func writeFakeFFmpeg(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "ffmpeg")
	script := "#!/bin/sh\n" +
		"echo \"Duration: 00:00:05.00, start: 0.000000, bitrate: 500 kb/s\" >&2\n" +
		"eval out=\\${$#}\n" +
		"echo \"fake mp4 bytes\" > \"$out\"\n" +
		"exit 0\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

// jobHandler mirrors cmd/worker/main.go's jobHandler (that type lives in an
// unimportable "main" package) — download, transcode, upload, then route the
// outcome through lifecycle.
type jobHandler struct {
	downloader *worker.Downloader
	transcoder *worker.Transcoder
	uploader   *storage.OutputUploader
	lifecycle  *worker.LifecycleHandler
}

func (h *jobHandler) HandleJob(ctx context.Context, msg queue.JobMessage) error {
	defer worker.CleanupTempDir(worker.NewLogger(io.Discard), "checkpoint-pod", msg.JobID)

	if err := h.lifecycle.HandleStart(ctx, msg.JobID); err != nil {
		return err
	}

	sourcePath, err := h.downloader.DownloadSourceFromS3(ctx, msg.JobID, msg.SourceS3URI)
	if err != nil {
		return err
	}
	destDir := filepath.Dir(sourcePath)

	job := pkg.Job{ID: msg.JobID, SourceS3URI: msg.SourceS3URI, Renditions: msg.Renditions, OutputS3Prefix: msg.OutputS3Prefix}
	singleResults := map[string]worker.RenditionResult{}
	var outFiles []storage.OutputFile
	for _, r := range job.Renditions {
		res, err := h.transcoder.TranscodeSingleRendition(ctx, msg.JobID, sourcePath, destDir, r)
		if err != nil {
			return err
		}
		singleResults[r.ID] = res
		outFiles = append(outFiles, storage.OutputFile{LocalPath: res.FilePath, Rendition: r.ID, Key: r.ID + "/" + filepath.Base(res.FilePath)})
	}

	if _, err := h.transcoder.GenerateManifest(ctx, job, singleResults, nil, destDir); err != nil {
		return err
	}
	manifestPath := filepath.Join(destDir, "manifest.json")
	if err := h.uploader.UploadOutputs(ctx, msg.JobID, outFiles, manifestPath); err != nil {
		return err
	}

	return h.lifecycle.HandleSuccess(ctx, msg)
}

// TestFullFlow_UploadThroughWorkerToStatus is the task 27 checkpoint: POST
// /videos/upload -> Kafka -> worker (download, transcode, upload, mark
// complete) -> GET /jobs/{id}, with S3/Kafka/Postgres all mocked. Verifies
// the job transitions submitted -> processing -> completed end to end, and
// that GET /jobs/{id} reflects it (including output_files from the
// worker-generated manifest).
func TestFullFlow_UploadThroughWorkerToStatus(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	// Shared backing stores for API and worker.
	db := newFakeDB()
	st := store.NewStore(db)
	broker := &fakeBroker{}
	outputS3 := newFakeOutputS3()

	// --- API side: real UploadHandler + real StatusHandler, wired to fakes.
	producer := queue.NewProducer(broker)
	uploadHandler := api.NewUploadHandler(fakeSourceUploader{}, producer, st, "pulsegrid-output-test", nil)
	manifestDownloader := storage.NewDownloader(outputS3, "pulsegrid-output-test")
	statusHandler := api.NewStatusHandler(st, manifestDownloader)

	// --- Submit the job via a real multipart POST.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	videoPart, err := mw.CreateFormFile("video", "clip.mp4")
	if err != nil {
		t.Fatalf("create video part: %v", err)
	}
	if _, err := videoPart.Write([]byte("fake video bytes")); err != nil {
		t.Fatalf("write video part: %v", err)
	}
	if err := mw.WriteField("source_name", "clip.mp4"); err != nil {
		t.Fatalf("write source_name field: %v", err)
	}
	renditionsJSON, _ := json.Marshal([]pkg.Rendition{{ID: "480p", Codec: "libx264", BitrateKbps: 1000, Width: 854, Height: 480}})
	if err := mw.WriteField("renditions", string(renditionsJSON)); err != nil {
		t.Fatalf("write renditions field: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	uploadHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("upload status = %d, want %d, body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var uploadResp api.UploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &uploadResp); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	jobID := uploadResp.JobID
	if jobID == "" {
		t.Fatal("upload response missing job_id")
	}

	// --- Job exists and is queued (per Requirement 1.6/2.1) before the
	// worker ever touches it.
	if len(broker.msgs) != 1 {
		t.Fatalf("broker received %d messages, want 1", len(broker.msgs))
	}
	statusReq := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID, nil)
	statusReq.SetPathValue("job_id", jobID)
	statusRec := httptest.NewRecorder()
	statusHandler.ServeHTTP(statusRec, statusReq)
	var queuedStatus api.StatusResponse
	if err := json.Unmarshal(statusRec.Body.Bytes(), &queuedStatus); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if queuedStatus.Status != string(pkg.JobStatusSubmitted) {
		t.Fatalf("pre-worker status = %q, want %q", queuedStatus.Status, pkg.JobStatusSubmitted)
	}

	// --- Worker side: real Consumer + a jobHandler assembled from the same
	// production pieces cmd/worker/main.go wires (Downloader, Transcoder,
	// OutputUploader, LifecycleHandler), all backed by the fakes above.
	ffmpegDir := t.TempDir()
	transcoder := worker.NewTranscoder()
	transcoder.SetFFmpegPath(writeFakeFFmpeg(t, ffmpegDir))

	handler := &jobHandler{
		downloader: worker.NewDownloader(fakeSourceS3Client{}),
		transcoder: transcoder,
		uploader:   storage.NewOutputUploader(outputS3, "pulsegrid-output-test"),
		lifecycle:  worker.NewLifecycleHandler(noopRetryPublisher{}, noopDLQPublisher{}, st, metrics.NewWorker(), "checkpoint-pod", worker.NewLogger(io.Discard)),
	}
	consumer := worker.NewConsumer(broker, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	deadline := time.After(9 * time.Second)
	for broker.committedCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("worker never committed the job's offset")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// --- Final status query: job must now read as completed, with the
	// worker-generated manifest's output_files populated.
	finalReq := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID, nil)
	finalReq.SetPathValue("job_id", jobID)
	finalRec := httptest.NewRecorder()
	statusHandler.ServeHTTP(finalRec, finalReq)

	if finalRec.Code != http.StatusOK {
		t.Fatalf("final status code = %d, body=%s", finalRec.Code, finalRec.Body.String())
	}
	var finalStatus api.StatusResponse
	if err := json.Unmarshal(finalRec.Body.Bytes(), &finalStatus); err != nil {
		t.Fatalf("decode final status response: %v", err)
	}
	if finalStatus.Status != string(pkg.JobStatusCompleted) {
		t.Fatalf("final status = %q, want %q", finalStatus.Status, pkg.JobStatusCompleted)
	}
	if finalStatus.CompletionTime == nil {
		t.Fatal("final status missing completion_time")
	}
	if len(finalStatus.OutputFiles) != 1 {
		t.Fatalf("final status output_files = %+v, want 1 entry", finalStatus.OutputFiles)
	}
	if finalStatus.OutputFiles[0].Rendition != "480p" {
		t.Fatalf("output file rendition = %q, want %q", finalStatus.OutputFiles[0].Rendition, "480p")
	}

	// --- Output objects landed at the correct S3 paths (Requirement 7.1),
	// including the manifest.
	outputS3.mu.Lock()
	_, hasRendition := outputS3.objects[jobID+"/480p/"+filepath.Base(finalStatus.OutputFiles[0].Path)]
	_, hasManifest := outputS3.objects[jobID+"/manifest.json"]
	outputS3.mu.Unlock()
	if !hasRendition {
		t.Errorf("output S3 missing rendition object under %s/480p/", jobID)
	}
	if !hasManifest {
		t.Errorf("output S3 missing %s/manifest.json", jobID)
	}
}
