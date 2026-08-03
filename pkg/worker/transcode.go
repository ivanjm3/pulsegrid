package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"pulsegrid/pkg"
)

// TranscodeTimeout bounds a single ffmpeg invocation. It matches the design
// doc's 30-minute default (long enough for the largest expected source
// files, short enough that a stuck ffmpeg process doesn't hold a worker
// forever).
const TranscodeTimeout = 30 * time.Minute

// durationRE matches ffmpeg's "Duration: HH:MM:SS.ss" line, emitted to
// stderr for every input.
var durationRE = regexp.MustCompile(`Duration:\s*(\d+):(\d+):(\d+)\.(\d+)`)

// RenditionResult is the metadata produced by a single successful rendition
// transcode, used to build the job manifest (task 16).
type RenditionResult struct {
	RenditionID     string
	FilePath        string
	FileSizeBytes   int64
	DurationSeconds int
}

// Transcoder invokes ffmpeg to produce transcoded outputs from a staged
// source file.
type Transcoder struct {
	// ffmpegPath is overridable in tests to point at a fake ffmpeg script.
	ffmpegPath string
	// timeout bounds each ffmpeg invocation; overridable in tests.
	timeout time.Duration
	// statFile returns the size of path in bytes; overridable in tests.
	statFile func(path string) (int64, error)
}

// NewTranscoder returns a Transcoder that shells out to the ffmpeg binary on
// PATH.
func NewTranscoder() *Transcoder {
	return &Transcoder{
		ffmpegPath: "ffmpeg",
		timeout:    TranscodeTimeout,
		statFile:   fileSize,
	}
}

// SetFFmpegPath overrides the ffmpeg binary path, letting callers outside
// this package (e.g. end-to-end tests) point the Transcoder at a fake
// ffmpeg script.
func (t *Transcoder) SetFFmpegPath(path string) {
	t.ffmpegPath = path
}

// TranscodeSingleRendition runs ffmpeg against sourceFile to produce a
// single MP4 rendition inside destDir. On a non-zero ffmpeg exit code it
// returns a *pkg.TranscodingError carrying the captured stderr.
func (t *Transcoder) TranscodeSingleRendition(ctx context.Context, jobID, sourceFile, destDir string, rendition pkg.Rendition) (RenditionResult, error) {
	outputFile := filepath.Join(destDir, rendition.ID+".mp4")

	args := []string{
		"-i", sourceFile,
		"-c:v", rendition.Codec,
		"-b:v", fmt.Sprintf("%dk", rendition.BitrateKbps),
		"-s", fmt.Sprintf("%dx%d", rendition.Width, rendition.Height),
		"-c:a", "aac",
		"-b:a", "128k",
		"-y",
		outputFile,
	}

	stderr, err := t.run(ctx, jobID, rendition.ID, args)
	if err != nil {
		return RenditionResult{}, err
	}

	size, err := t.statFile(outputFile)
	if err != nil {
		return RenditionResult{}, fmt.Errorf("transcode single rendition %s: stat output: %w", rendition.ID, err)
	}

	return RenditionResult{
		RenditionID:     rendition.ID,
		FilePath:        outputFile,
		FileSizeBytes:   size,
		DurationSeconds: extractDurationSeconds(stderr),
	}, nil
}

// run executes ffmpeg with args under t.timeout, returning captured stderr.
// A non-zero exit code (including a timeout, which kills the process) is
// reported as a *pkg.TranscodingError.
func (t *Transcoder) run(ctx context.Context, jobID, renditionID string, args []string) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, t.ffmpegPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// ffmpeg (or a test double shell script) may hold stdout/stderr open via
	// child processes even after being killed itself, which would otherwise
	// block Wait() until those children exit on their own. Run it in its own
	// process group so Cancel can kill the whole group, and cap how long
	// Wait() will wait for pipes to close before giving up.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 5 * time.Second

	err := cmd.Run()
	if err != nil {
		if runCtx.Err() == context.DeadlineExceeded {
			return stderr.String(), &pkg.TranscodingError{
				JobID:     jobID,
				Rendition: renditionID,
				Stderr:    stderr.String(),
				Err:       fmt.Errorf("ffmpeg timed out after %s", t.timeout),
			}
		}
		return stderr.String(), &pkg.TranscodingError{
			JobID:     jobID,
			Rendition: renditionID,
			Stderr:    stderr.String(),
			Err:       err,
		}
	}

	return stderr.String(), nil
}

// extractDurationSeconds parses ffmpeg's "Duration: HH:MM:SS.ss" line from
// stderr output, returning 0 if not found.
func extractDurationSeconds(stderr string) int {
	m := durationRE.FindStringSubmatch(stderr)
	if m == nil {
		return 0
	}
	h, _ := strconv.Atoi(m[1])
	mi, _ := strconv.Atoi(m[2])
	s, _ := strconv.Atoi(m[3])
	return h*3600 + mi*60 + s
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
