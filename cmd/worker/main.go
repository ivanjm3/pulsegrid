// Command worker runs a Pulsegrid worker pod: it consumes transcoding jobs
// from Kafka, stages source videos to local disk, transcodes renditions,
// uploads outputs, and routes completion/retry/DLQ outcomes.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"pulsegrid/pkg"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/storage"
	"pulsegrid/pkg/store"
	"pulsegrid/pkg/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)
	downloader := worker.NewDownloader(s3Client)

	outputBucket := envOrDefault("S3_BUCKET_OUTPUT", "pulsegrid-output")
	uploader := storage.NewOutputUploader(s3Client, outputBucket)
	transcoder := worker.NewTranscoder()

	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	reader := worker.NewKafkaReader(brokers)

	jobWriter := queue.NewKafkaWriter(brokers)
	retryProducer := queue.NewProducer(jobWriter)
	defer retryProducer.Close()

	dlqWriter := queue.NewKafkaDLQWriter(brokers)
	dlqProducer := queue.NewDLQProducer(dlqWriter)
	defer dlqProducer.Close()

	pool, err := store.Connect(ctx, os.Getenv("DB_DSN"))
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()
	db := store.NewStore(pool)

	m := metrics.NewWorker()
	podID := envOrDefault("HOSTNAME", "unknown")
	logger := worker.NewLogger(os.Stderr)
	lifecycle := worker.NewLifecycleHandler(retryProducer, dlqProducer, db, m, podID, logger)

	handler := &jobHandler{
		downloader: downloader,
		transcoder: transcoder,
		uploader:   uploader,
		lifecycle:  lifecycle,
		metrics:    m,
		logger:     logger,
		podID:      podID,
	}
	consumer := worker.NewConsumer(reader, handler)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", m.Handler())
	go func() {
		const metricsAddr = ":8081"
		log.Printf("pulsegrid worker metrics listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, metricsMux); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("pulsegrid worker starting: brokers=%v group=%s", brokers, worker.GroupID)
	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("consumer exited: %v", err)
	}
	log.Print("pulsegrid worker shut down cleanly")
}

// jobHandler runs a job end-to-end: download source, transcode every
// requested rendition, upload outputs, then route the outcome through
// lifecycle (completion, retry, or DLQ — task 18). The job's staging
// directory is always removed afterward, on both the success and failure
// paths (task 19).
type jobHandler struct {
	downloader *worker.Downloader
	transcoder *worker.Transcoder
	uploader   *storage.OutputUploader
	lifecycle  *worker.LifecycleHandler
	metrics    *metrics.WorkerMetrics
	logger     *slog.Logger
	podID      string
}

func (h *jobHandler) HandleJob(ctx context.Context, msg queue.JobMessage) error {
	defer worker.CleanupTempDir(h.logger, h.podID, msg.JobID)

	procErr := h.process(ctx, msg)
	if procErr == nil {
		if err := h.lifecycle.HandleSuccess(ctx, msg); err != nil {
			worker.LogJobError(h.logger, "record_status_event_failed", msg.JobID, h.podID, err, msg.RetryCount, "", "")
		}
		log.Printf("event=job_completed job_id=%s", msg.JobID)
		return nil
	}

	outcome, err := h.lifecycle.HandleFailure(ctx, msg, procErr)
	if err != nil {
		worker.LogJobError(h.logger, "lifecycle_handling_failed", msg.JobID, h.podID, err, msg.RetryCount, "", "")
		return err
	}

	log.Printf("event=job_failed job_id=%s outcome=%s error=%v", msg.JobID, outcome, procErr)
	if outcome == worker.OutcomeConstrained {
		log.Printf("event=pod_exiting_resource_constrained job_id=%s", msg.JobID)
		os.Exit(1)
	}
	return nil
}

// process downloads the job's source video, transcodes every requested
// rendition, generates the manifest, and uploads all outputs to S3.
func (h *jobHandler) process(ctx context.Context, msg queue.JobMessage) error {
	sourcePath, err := h.downloader.DownloadSourceFromS3(ctx, msg.JobID, msg.SourceS3URI)
	if err != nil {
		return err
	}
	destDir := filepath.Dir(sourcePath)

	job := pkg.Job{
		ID:             msg.JobID,
		SourceS3URI:    msg.SourceS3URI,
		Renditions:     msg.Renditions,
		OutputS3Prefix: msg.OutputS3Prefix,
	}

	singleResults := map[string]worker.RenditionResult{}
	hlsResults := map[string]worker.HLSResult{}
	var outFiles []storage.OutputFile

	for _, r := range job.Renditions {
		start := time.Now()
		if r.HLS {
			res, err := h.transcoder.TranscodeHLS(ctx, msg.JobID, sourcePath, destDir, r)
			if err != nil {
				return err
			}
			h.metrics.ObserveTranscodeDuration(r.ID, time.Since(start).Seconds())
			hlsResults[r.ID] = res

			segments, err := filepath.Glob(filepath.Join(filepath.Dir(res.PlaylistPath), "*.ts"))
			if err != nil {
				return fmt.Errorf("list hls segments for rendition %s: %w", r.ID, err)
			}
			outFiles = append(outFiles, storage.OutputFile{
				LocalPath: res.PlaylistPath,
				Rendition: r.ID,
				Key:       fmt.Sprintf("%s/playlist.m3u8", r.ID),
			})
			for _, seg := range segments {
				outFiles = append(outFiles, storage.OutputFile{
					LocalPath: seg,
					Rendition: r.ID,
					Key:       fmt.Sprintf("%s/%s", r.ID, filepath.Base(seg)),
				})
			}
			continue
		}

		res, err := h.transcoder.TranscodeSingleRendition(ctx, msg.JobID, sourcePath, destDir, r)
		if err != nil {
			return err
		}
		h.metrics.ObserveTranscodeDuration(r.ID, time.Since(start).Seconds())
		singleResults[r.ID] = res
		outFiles = append(outFiles, storage.OutputFile{
			LocalPath: res.FilePath,
			Rendition: r.ID,
			Key:       fmt.Sprintf("%s/%s", r.ID, filepath.Base(res.FilePath)),
		})
	}

	if _, err := h.transcoder.GenerateManifest(ctx, job, singleResults, hlsResults, destDir); err != nil {
		return err
	}

	manifestPath := filepath.Join(destDir, "manifest.json")
	if err := h.uploader.UploadOutputs(ctx, msg.JobID, outFiles, manifestPath); err != nil {
		return err
	}

	return nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
