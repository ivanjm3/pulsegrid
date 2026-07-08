package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"pulsegrid/pkg"
)

// s3Downloader is the S3 download client (nil = local dev, no download).
var s3Downloader pkg.S3Downloader

// s3OutputUploader is the S3 output upload client (nil = local dev, no upload).
var s3OutputUploader pkg.S3OutputUploader

// kafkaProducer is used for re-enqueue and DLQ publishing.
var kafkaProducer pkg.KafkaProducer

// workerMetrics holds worker-specific Prometheus metrics.
var workerMetrics *pkg.WorkerMetrics

// logger is the structured JSON logger for the worker.
var logger *pkg.Logger

// maxRetries is the maximum retry count before sending to DLQ.
const maxRetries = 3

func main() {
	// Initialize structured logger
	logger = pkg.NewLogger()
	defer logger.Sync()

	// Configuration from environment
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("JOB_TOPIC", "transcoding-jobs")
	dlqTopic := getEnv("DLQ_TOPIC", "transcoding-dlq")
	groupID := getEnv("CONSUMER_GROUP", "pulsegrid-workers")
	podID := logger.PodID()

	brokerList := strings.Split(brokers, ",")

	logger.Info("worker starting", "worker_startup",
		zap.String("topic", topic),
		zap.String("group", groupID),
		zap.Strings("brokers", brokerList),
	)

	// Initialize worker metrics
	workerMetrics = pkg.NewWorkerMetrics(nil)

	// Initialize S3 download client
	awsRegion := getEnv("AWS_REGION", getEnv("AWS_DEFAULT_REGION", ""))
	if awsRegion != "" {
		cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(awsRegion))
		if err != nil {
			logger.Error("failed to load AWS config, S3 downloads disabled", "aws_config_error", err,
				zap.String("region", awsRegion),
			)
		} else {
			bucket := getEnv("PULSEGRID_SOURCE_BUCKET", "pulsegrid-source")
			s3Client := pkg.NewS3Client(cfg, bucket)
			s3Downloader = s3Client
			s3OutputUploader = pkg.NewS3Client(cfg, "pulsegrid-output")
			logger.Info("s3 clients initialized", "s3_init",
				zap.String("region", awsRegion),
				zap.String("source_bucket", bucket),
				zap.String("output_bucket", "pulsegrid-output"),
			)
		}
	} else {
		logger.Info("AWS_REGION not set, S3 disabled (local dev mode)", "s3_disabled")
	}

	// Initialize Kafka producer for re-enqueue and DLQ
	kafkaProducer = pkg.NewKafkaClient(brokerList, topic, dlqTopic)
	defer kafkaProducer.Close()

	// Initialize Kafka consumer (Reader with consumer group)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           brokerList,
		Topic:             topic,
		GroupID:           groupID,
		StartOffset:       kafka.FirstOffset,
		SessionTimeout:    30 * time.Minute,
		HeartbeatInterval: 3 * time.Second,
		MaxWait:           5 * time.Second,
	})
	defer reader.Close()

	// Shutdown coordination
	var shutdownRequested atomic.Bool
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGTERM, syscall.SIGINT)

	// Start Prometheus metrics endpoint on :8081
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: ":8081", Handler: metricsMux}
	go func() {
		logger.Info("metrics server listening on :8081", "metrics_server_start")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "metrics_server_error", err)
		}
	}()

	// Listen for shutdown signal in background
	go func() {
		sig := <-shutdownCh
		logger.Info("received signal, initiating graceful shutdown", "shutdown_signal",
			zap.String("signal", sig.String()),
		)
		shutdownRequested.Store(true)
	}()

	logger.Info("worker ready, entering polling loop", "worker_ready")

	// Main polling loop
	for {
		if shutdownRequested.Load() {
			logger.Info("shutdown requested, exiting polling loop", "shutdown_exit")
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := reader.FetchMessage(ctx)
		cancel()

		if err != nil {
			// Timeout or context cancelled — no message available
			if ctx.Err() != nil {
				continue
			}
			// Check if shutdown was requested during fetch
			if shutdownRequested.Load() {
				break
			}
			logger.Error("fetch error", "kafka_fetch_error", err)
			continue
		}

		// Deserialize KafkaMessage
		var kafkaMsg pkg.KafkaMessage
		if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			logger.Error("failed to deserialize message", "message_deserialize_error", err,
				zap.Int64("offset", msg.Offset),
				zap.Int("partition", msg.Partition),
			)
			// Commit to skip malformed message
			if commitErr := reader.CommitMessages(context.Background(), msg); commitErr != nil {
				logger.Error("commit error for malformed message", "kafka_commit_error", commitErr)
			}
			continue
		}

		logger.LogJobEvent("processing job", kafkaMsg.JobID, "job_processing_start",
			zap.String("source", kafkaMsg.SourceS3URI),
			zap.Int("renditions", len(kafkaMsg.Renditions)),
			zap.Int("retry_count", kafkaMsg.RetryCount),
		)

		// Process job and handle outcome
		processErr := processJob(kafkaMsg, podID)
		handleJobOutcome(context.Background(), reader, msg, kafkaMsg, processErr, podID)
	}

	// Graceful shutdown: close metrics server and consumer
	logger.Info("shutting down metrics server", "metrics_server_shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "metrics_server_shutdown_error", err)
	}

	logger.Info("closing kafka consumer", "kafka_consumer_close")
	if err := reader.Close(); err != nil {
		logger.Error("kafka consumer close error", "kafka_consumer_close_error", err)
	}

	logger.Info("worker shutdown complete", "worker_shutdown_complete")
}

// handleJobOutcome routes job result through error classification to success/retry/DLQ/exit paths.
func handleJobOutcome(ctx context.Context, reader *kafka.Reader, msg kafka.Message, kafkaMsg pkg.KafkaMessage, processErr error, podID string) {
	if processErr == nil {
		// SUCCESS: commit offset, emit completed metric, record status event
		if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
			logger.LogJobError("commit error", kafkaMsg.JobID, "kafka_commit_error", commitErr)
		} else {
			logger.LogJobEvent("job completed", kafkaMsg.JobID, "job_completed",
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
			)
		}
		workerMetrics.JobCompletedTotal.Inc()
		return
	}

	// Classify error
	errType := pkg.ClassifyError(processErr)

	// Extract stderr from TranscodingError if available
	stderr := ""
	var te *pkg.TranscodingError
	if errors.As(processErr, &te) {
		stderr = te.Stderr
	}

	// Log failure with full context
	logger.LogTranscodeFailure("job failed", kafkaMsg.JobID, "job_failed", processErr, stderr, kafkaMsg.RetryCount, errType)

	// Emit failure metric with error_type label
	workerMetrics.TranscodeFailureTotal.WithLabelValues(string(errType)).Inc()

	switch errType {
	case pkg.ErrorTypeConstraint:
		// Resource constraint: log, emit metric, exit pod (don't commit — let Kafka rebalance)
		logger.LogJobError("resource constraint detected, pod must exit", kafkaMsg.JobID, "pod_resource_constrained", processErr)
		workerMetrics.PodResourceConstrained.Inc()
		os.Exit(1)

	case pkg.ErrorTypePermanent:
		// Permanent error: send to DLQ immediately, no retry
		logger.LogJobEvent("permanent error, sending to DLQ", kafkaMsg.JobID, "job_dlq_permanent",
			zap.String("error_type", string(errType)),
		)
		if dlqErr := kafkaProducer.SendDLQFromMessage(ctx, kafkaMsg, processErr.Error(), podID); dlqErr != nil {
			logger.LogJobError("DLQ publish failed", kafkaMsg.JobID, "dlq_publish_error", dlqErr)
		}
		// Commit offset — message handled (in DLQ now)
		if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
			logger.LogJobError("commit error for DLQ job", kafkaMsg.JobID, "kafka_commit_error", commitErr)
		}

	case pkg.ErrorTypeRetryable:
		// Retryable: check retry_count
		if kafkaMsg.RetryCount >= maxRetries {
			// Max retries exceeded — send to DLQ
			logger.LogJobEvent("max retries exceeded, sending to DLQ", kafkaMsg.JobID, "job_dlq_max_retries",
				zap.Int("retry_count", kafkaMsg.RetryCount),
				zap.Int("max_retries", maxRetries),
			)
			if dlqErr := kafkaProducer.SendDLQFromMessage(ctx, kafkaMsg, processErr.Error(), podID); dlqErr != nil {
				logger.LogJobError("DLQ publish failed", kafkaMsg.JobID, "dlq_publish_error", dlqErr)
			}
		} else {
			// Increment retry_count, re-enqueue
			kafkaMsg.RetryCount++
			logger.LogJobEvent("retryable error, re-enqueueing", kafkaMsg.JobID, "job_retry",
				zap.Int("retry_count", kafkaMsg.RetryCount),
				zap.Int("max_retries", maxRetries),
			)
			if reenqErr := kafkaProducer.ReenqueueWithRetry(ctx, kafkaMsg); reenqErr != nil {
				logger.LogJobError("reenqueue failed", kafkaMsg.JobID, "reenqueue_error", reenqErr)
			}
		}
		// Commit original offset — we re-enqueued or DLQ'd
		if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
			logger.LogJobError("commit error for retried job", kafkaMsg.JobID, "kafka_commit_error", commitErr)
		}
	}
}

// cleanupTempDir removes /tmp/{jobID} directory recursively.
// Logs result, handles permission errors gracefully (no panic).
func cleanupTempDir(tempDir string, jobID string) {
	err := os.RemoveAll(tempDir)
	if err != nil {
		if os.IsPermission(err) {
			logger.LogJobError("cleanup permission denied", jobID, "cleanup_permission_error", err,
				zap.String("path", tempDir),
			)
		} else {
			logger.LogJobError("cleanup failed", jobID, "cleanup_error", err,
				zap.String("path", tempDir),
			)
		}
		return
	}
	logger.LogJobEvent("cleanup complete", jobID, "cleanup_complete",
		zap.String("path", tempDir),
	)
}

// processJob downloads source from S3 and transcodes each rendition.
func processJob(msg pkg.KafkaMessage, podID string) error {
	logger.LogJobEvent("processing job", msg.JobID, "job_process_start")

	ctx := context.Background()
	start := time.Now()

	// Step 1: Download source from S3
	if s3Downloader == nil {
		logger.LogJobEvent("s3 downloader not configured, skipping (local dev)", msg.JobID, "s3_skip_local_dev")
		return nil
	}

	localPath, err := s3Downloader.DownloadSourceFromS3(ctx, msg.JobID, msg.SourceS3URI)
	if err != nil {
		return fmt.Errorf("source download failed: %w", err)
	}

	// Defer cleanup of temp directory — runs on both success and failure
	tempDir := filepath.Dir(localPath)
	defer cleanupTempDir(tempDir, msg.JobID)

	logger.LogJobEvent("source downloaded", msg.JobID, "source_downloaded",
		zap.String("path", localPath),
	)

	// Step 2: Transcode each non-HLS rendition
	var results []*pkg.TranscodeResult
	for _, rendition := range msg.Renditions {
		if rendition.Type == "hls_segments" {
			continue // HLS handled separately
		}

		result, err := pkg.TranscodeSingleRendition(ctx, localPath, rendition, msg.JobID)
		if err != nil {
			return fmt.Errorf("transcode failed for rendition %s: %w", rendition.ID, err)
		}

		elapsed := time.Since(start).Seconds()
		workerMetrics.TranscodeDurationSeconds.WithLabelValues(rendition.ID).Observe(elapsed)

		logger.LogJobEvent("rendition done", msg.JobID, "rendition_completed",
			zap.String("rendition_id", result.RenditionID),
			zap.Int64("size", result.FileSize),
			zap.Float64("duration_seconds", result.DurationSeconds),
		)
		results = append(results, result)
	}

	// Step 2b: Transcode HLS renditions
	var hlsResults []*pkg.HLSResult
	for _, rendition := range msg.Renditions {
		if rendition.Type != "hls_segments" {
			continue
		}

		hlsResult, err := pkg.TranscodeHLS(ctx, localPath, rendition, msg.JobID)
		if err != nil {
			return fmt.Errorf("hls transcode failed for rendition %s: %w", rendition.ID, err)
		}

		elapsed := time.Since(start).Seconds()
		workerMetrics.TranscodeDurationSeconds.WithLabelValues(rendition.ID).Observe(elapsed)

		logger.LogJobEvent("hls rendition done", msg.JobID, "hls_rendition_completed",
			zap.String("rendition_id", hlsResult.RenditionID),
			zap.Int("segment_count", hlsResult.SegmentCount),
		)
		hlsResults = append(hlsResults, hlsResult)
	}

	logger.LogJobEvent("all renditions complete", msg.JobID, "all_renditions_complete",
		zap.Int("mp4_count", len(results)),
		zap.Int("hls_count", len(hlsResults)),
	)

	// Step 3: Generate manifest
	manifestPath, err := pkg.GenerateManifest(ctx, msg.JobID, msg.SourceS3URI, results, hlsResults, tempDir)
	if err != nil {
		return fmt.Errorf("manifest generation failed: %w", err)
	}
	logger.LogJobEvent("manifest generated", msg.JobID, "manifest_generated",
		zap.String("path", manifestPath),
	)

	// Step 4: Upload outputs to S3
	if s3OutputUploader != nil {
		if err := s3OutputUploader.UploadOutputsToS3(ctx, msg.JobID, results, hlsResults, manifestPath); err != nil {
			return fmt.Errorf("output upload failed: %w", err)
		}
		logger.LogJobEvent("outputs uploaded to s3", msg.JobID, "outputs_uploaded")
	} else {
		logger.LogJobEvent("s3 output uploader not configured, skipping (local dev)", msg.JobID, "upload_skip_local_dev")
	}

	return nil
}

// getEnv returns env variable value or fallback default.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
