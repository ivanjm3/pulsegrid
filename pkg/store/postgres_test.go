package store

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pulsegrid/pkg"
)

// fakeRow implements pgx.Row over a fixed set of column values, in the exact
// order GetJob's SELECT lists them.
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
	if len(dest) != len(values) {
		return errors.New("fakeRow: column count mismatch")
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
		default:
			return errors.New("fakeRow: unsupported dest type")
		}
	}
	return nil
}

// fakeJobRow mirrors a single row of the jobs table.
type fakeJobRow struct {
	jobID               string
	status              string
	sourceFileName      string
	sourceFileSizeBytes int64
	sourceS3URI         string
	outputS3Prefix      string
	requestedRenditions []byte
	submissionTime      time.Time
	completionTime      *time.Time
	retryCount          int
	failureReason       *string
}

// fakeDB is an in-memory stand-in for *pgxpool.Pool implementing DB, used to
// test Store without a live Postgres instance.
type fakeDB struct {
	jobs         map[string]fakeJobRow
	statusEvents int
}

func newFakeDB() *fakeDB {
	return &fakeDB{jobs: make(map[string]fakeJobRow)}
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(sql, "INSERT INTO jobs"):
		if _, exists := f.jobs[args[0].(string)]; exists {
			return pgconn.CommandTag{}, errors.New(`duplicate key value violates unique constraint "jobs_pkey"`)
		}
		f.jobs[args[0].(string)] = fakeJobRow{
			jobID:               args[0].(string),
			status:              args[1].(string),
			sourceFileName:      args[2].(string),
			sourceFileSizeBytes: args[3].(int64),
			sourceS3URI:         args[4].(string),
			outputS3Prefix:      args[5].(string),
			requestedRenditions: args[6].([]byte),
			submissionTime:      args[7].(time.Time),
			retryCount:          args[8].(int),
		}
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "UPDATE jobs SET status"):
		jobID := args[0].(string)
		row, exists := f.jobs[jobID]
		if !exists {
			return pgconn.CommandTag{}, nil
		}
		row.status = args[1].(string)
		f.jobs[jobID] = row
		return pgconn.CommandTag{}, nil

	case strings.Contains(sql, "INSERT INTO job_status_events"):
		f.statusEvents++
		return pgconn.CommandTag{}, nil
	}
	return pgconn.CommandTag{}, errors.New("fakeDB: unrecognized statement")
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "SELECT COUNT(*)") {
		return &fakeCountRow{count: len(f.jobs)}
	}
	if strings.Contains(sql, "FROM jobs") {
		row, exists := f.jobs[args[0].(string)]
		return &fakeRow{row: row, ok: exists}
	}
	return &fakeRow{ok: false}
}

// fakeCountRow implements pgx.Row for a single COUNT(*) column.
type fakeCountRow struct{ count int }

func (r *fakeCountRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = r.count
	return nil
}

// fakeRows implements pgx.Rows over a fixed slice of jobs rows, for ListJobs.
type fakeRows struct {
	rows []fakeJobRow
	i    int
}

func (r *fakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	*(dest[0].(*string)) = row.jobID
	*(dest[1].(*string)) = row.status
	*(dest[2].(*time.Time)) = row.submissionTime
	*(dest[3].(**time.Time)) = row.completionTime
	return nil
}

func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) Close()                                       {}
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

func (f *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows := make([]fakeJobRow, 0, len(f.jobs))
	for _, row := range f.jobs {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].submissionTime.After(rows[j].submissionTime) })
	return &fakeRows{rows: rows}, nil
}

func TestRecordJobMetadata_InsertsRow(t *testing.T) {
	db := newFakeDB()
	s := NewStore(db)

	job := pkg.Job{
		ID:                  "11111111-1111-4111-8111-111111111111",
		SourceName:          "clip.mp4",
		SourceFileSizeBytes: 1024,
		SourceS3URI:         "s3://pulsegrid-source/11111111-1111-4111-8111-111111111111/original.mp4",
		OutputS3Prefix:      "s3://pulsegrid-output/11111111-1111-4111-8111-111111111111/",
		Status:              pkg.JobStatusSubmitted,
		Renditions:          []pkg.Rendition{{ID: "720p", Codec: "libx264", BitrateKbps: 5000, Width: 1280, Height: 720}},
		SubmissionTime:      time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	if err := s.RecordJobMetadata(context.Background(), job); err != nil {
		t.Fatalf("RecordJobMetadata: %v", err)
	}
	if len(db.jobs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(db.jobs))
	}
}

func TestGetJob_NotFound(t *testing.T) {
	db := newFakeDB()
	s := NewStore(db)

	if _, err := s.GetJob(context.Background(), "does-not-exist"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected wrapped pgx.ErrNoRows, got %v", err)
	}
}

func TestUpdateJobStatus_ChangesStatus(t *testing.T) {
	db := newFakeDB()
	s := NewStore(db)

	job := pkg.Job{
		ID:             "22222222-2222-4222-8222-222222222222",
		SourceName:     "clip.mp4",
		SourceS3URI:    "s3://pulsegrid-source/22222222.../original.mp4",
		OutputS3Prefix: "s3://pulsegrid-output/22222222.../",
		Status:         pkg.JobStatusSubmitting,
		Renditions:     []pkg.Rendition{},
		SubmissionTime: time.Now().UTC(),
	}
	ctx := context.Background()
	if err := s.RecordJobMetadata(ctx, job); err != nil {
		t.Fatalf("RecordJobMetadata: %v", err)
	}
	if err := s.UpdateJobStatus(ctx, job.ID, pkg.JobStatusSubmitted); err != nil {
		t.Fatalf("UpdateJobStatus: %v", err)
	}

	got, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != pkg.JobStatusSubmitted {
		t.Fatalf("expected status %q, got %q", pkg.JobStatusSubmitted, got.Status)
	}
}

func TestListJobs_ReturnsInsertedJobsWithTotal(t *testing.T) {
	db := newFakeDB()
	s := NewStore(db)
	ctx := context.Background()

	jobs := []pkg.Job{
		{ID: "11111111-1111-4111-8111-111111111111", SourceName: "a.mp4", SourceS3URI: "s3://x/a", OutputS3Prefix: "s3://y/a/", Status: pkg.JobStatusCompleted, Renditions: []pkg.Rendition{}, SubmissionTime: time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)},
		{ID: "22222222-2222-4222-8222-222222222222", SourceName: "b.mp4", SourceS3URI: "s3://x/b", OutputS3Prefix: "s3://y/b/", Status: pkg.JobStatusFailed, Renditions: []pkg.Rendition{}, SubmissionTime: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)},
	}
	for _, j := range jobs {
		if err := s.RecordJobMetadata(ctx, j); err != nil {
			t.Fatalf("RecordJobMetadata: %v", err)
		}
	}

	results, total, err := s.ListJobs(ctx, JobFilter{Limit: 100, Offset: 0})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
}

func TestRecordStatusEvent_Inserts(t *testing.T) {
	db := newFakeDB()
	s := NewStore(db)

	err := s.RecordStatusEvent(context.Background(), "job-1", "submitted", map[string]any{"foo": "bar"}, "worker-pod-1")
	if err != nil {
		t.Fatalf("RecordStatusEvent: %v", err)
	}
	if db.statusEvents != 1 {
		t.Fatalf("expected 1 status event, got %d", db.statusEvents)
	}
}

// randomJob generates a random job for property testing, mirroring the style
// used in pkg/queue's message schema property test.
func randomJob(r *rand.Rand) pkg.Job {
	id, err := pkg.NewJobID()
	if err != nil {
		panic(err)
	}

	n := r.Intn(6) // 0-5 renditions
	renditions := make([]pkg.Rendition, n)
	codecs := []string{"libx264", "libx265", "vp9"}
	for i := 0; i < n; i++ {
		if r.Intn(4) == 0 {
			renditions[i] = pkg.Rendition{ID: "hls", HLS: true}
			continue
		}
		renditions[i] = pkg.Rendition{
			ID:          "rendition-" + string(rune('a'+i)),
			Codec:       codecs[r.Intn(len(codecs))],
			BitrateKbps: 100 + r.Intn(8000),
			Width:       [3]int{1280, 854, 640}[r.Intn(3)],
			Height:      [3]int{720, 480, 360}[r.Intn(3)],
		}
	}

	statuses := []pkg.JobStatus{pkg.JobStatusSubmitting, pkg.JobStatusSubmitted, pkg.JobStatusProcessing, pkg.JobStatusCompleted, pkg.JobStatusFailed}

	return pkg.Job{
		ID:                  id,
		SourceName:          "source-" + id + ".mp4",
		SourceFileSizeBytes: int64(r.Intn(10_000_000_000)),
		SourceS3URI:         "s3://pulsegrid-source/" + id + "/original.mp4",
		OutputS3Prefix:      "s3://pulsegrid-output/" + id + "/",
		Renditions:          renditions,
		Status:              statuses[r.Intn(len(statuses))],
		RetryCount:          r.Intn(4),
		SubmissionTime:      time.Now().UTC().Truncate(time.Microsecond),
	}
}

// TestDatabasePersistenceRoundTrip is Property 7: for any uploaded job, the
// job metadata SHALL be recorded to Postgres and a subsequent query SHALL
// return matching status, submission_time, source_file_name (via
// SourceName), and requested_renditions.
//
// Validates: Requirements 5.1, 5.5
func TestDatabasePersistenceRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		db := newFakeDB()
		s := NewStore(db)

		job := randomJob(r)

		if err := s.RecordJobMetadata(ctx, job); err != nil {
			t.Fatalf("iteration %d: RecordJobMetadata: %v", i, err)
		}

		got, err := s.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("iteration %d: GetJob: %v", i, err)
		}

		if got.ID != job.ID {
			t.Fatalf("iteration %d: job_id mismatch: got %q want %q", i, got.ID, job.ID)
		}
		if got.Status != job.Status {
			t.Fatalf("iteration %d: status mismatch: got %q want %q", i, got.Status, job.Status)
		}
		if !got.SubmissionTime.Equal(job.SubmissionTime) {
			t.Fatalf("iteration %d: submission_time mismatch: got %v want %v", i, got.SubmissionTime, job.SubmissionTime)
		}
		if got.SourceName != job.SourceName {
			t.Fatalf("iteration %d: source_file_name mismatch: got %q want %q", i, got.SourceName, job.SourceName)
		}
		if got.SourceFileSizeBytes != job.SourceFileSizeBytes {
			t.Fatalf("iteration %d: source_file_size_bytes mismatch: got %d want %d", i, got.SourceFileSizeBytes, job.SourceFileSizeBytes)
		}
		if got.SourceS3URI != job.SourceS3URI {
			t.Fatalf("iteration %d: source_s3_uri mismatch", i)
		}
		if got.OutputS3Prefix != job.OutputS3Prefix {
			t.Fatalf("iteration %d: output_s3_prefix mismatch", i)
		}
		if got.RetryCount != job.RetryCount {
			t.Fatalf("iteration %d: retry_count mismatch: got %d want %d", i, got.RetryCount, job.RetryCount)
		}
		if len(got.Renditions) != len(job.Renditions) {
			t.Fatalf("iteration %d: renditions length mismatch: got %d want %d", i, len(got.Renditions), len(job.Renditions))
		}
		for j := range job.Renditions {
			if got.Renditions[j] != job.Renditions[j] {
				t.Fatalf("iteration %d: rendition %d mismatch: got %+v want %+v", i, j, got.Renditions[j], job.Renditions[j])
			}
		}
	}
}
