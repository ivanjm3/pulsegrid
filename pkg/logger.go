package pkg

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.Logger with pod_id context for structured JSON logging.
type Logger struct {
	*zap.Logger
	podID string
}

// NewLogger creates a production JSON logger with ISO 8601 timestamps.
// podID is read from HOSTNAME env var (Kubernetes injects this).
func NewLogger() *Logger {
	podID := os.Getenv("HOSTNAME")
	if podID == "" {
		podID = os.Getenv("POD_ID")
	}
	if podID == "" {
		podID = "worker-unknown"
	}

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		CallerKey:      "caller",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout(time.RFC3339),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(os.Stdout),
		zap.InfoLevel,
	)

	logger := zap.New(core, zap.AddCaller())

	return &Logger{
		Logger: logger,
		podID:  podID,
	}
}

// PodID returns the pod identifier for this logger.
func (l *Logger) PodID() string {
	return l.podID
}

// Info logs an informational event with pod_id and event_type.
func (l *Logger) Info(msg string, eventType string, fields ...zap.Field) {
	allFields := append([]zap.Field{
		zap.String("pod_id", l.podID),
		zap.String("event_type", eventType),
	}, fields...)
	l.Logger.Info(msg, allFields...)
}

// Error logs an error event with pod_id, event_type, and error_message.
func (l *Logger) Error(msg string, eventType string, err error, fields ...zap.Field) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	allFields := append([]zap.Field{
		zap.String("pod_id", l.podID),
		zap.String("event_type", eventType),
		zap.String("error_message", errMsg),
	}, fields...)
	l.Logger.Error(msg, allFields...)
}

// LogJobEvent logs a job-specific event with job_id, pod_id, event_type.
func (l *Logger) LogJobEvent(msg string, jobID string, eventType string, fields ...zap.Field) {
	allFields := append([]zap.Field{
		zap.String("job_id", jobID),
		zap.String("pod_id", l.podID),
		zap.String("event_type", eventType),
	}, fields...)
	l.Logger.Info(msg, allFields...)
}

// LogJobError logs a job error with full context: job_id, pod_id, event_type, error_message.
func (l *Logger) LogJobError(msg string, jobID string, eventType string, err error, fields ...zap.Field) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	allFields := append([]zap.Field{
		zap.String("job_id", jobID),
		zap.String("pod_id", l.podID),
		zap.String("event_type", eventType),
		zap.String("error_message", errMsg),
	}, fields...)
	l.Logger.Error(msg, allFields...)
}

// LogTranscodeFailure logs a transcoding failure with ffmpeg_stderr, retry_count, error_type.
func (l *Logger) LogTranscodeFailure(msg string, jobID string, eventType string, err error, stderr string, retryCount int, errorType ErrorType) {
	// Truncate stderr to first 500 chars
	if len(stderr) > 500 {
		stderr = stderr[:500]
	}

	l.Logger.Error(msg,
		zap.String("job_id", jobID),
		zap.String("pod_id", l.podID),
		zap.String("event_type", eventType),
		zap.String("error_message", err.Error()),
		zap.String("ffmpeg_stderr", stderr),
		zap.Int("retry_count", retryCount),
		zap.String("error_type", string(errorType)),
	)
}
