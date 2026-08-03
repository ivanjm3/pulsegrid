// Command worker runs a Pulsegrid worker pod: it consumes transcoding jobs
// from Kafka, stages source videos to local disk, and (in later stages of
// the build) transcodes and uploads outputs.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"pulsegrid/pkg/queue"
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

	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	reader := worker.NewKafkaReader(brokers)

	handler := &jobHandler{downloader: downloader}
	consumer := worker.NewConsumer(reader, handler)

	log.Printf("pulsegrid worker starting: brokers=%v group=%s", brokers, worker.GroupID)
	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("consumer exited: %v", err)
	}
	log.Print("pulsegrid worker shut down cleanly")
}

// jobHandler downloads a job's source video ahead of transcoding. Later
// build stages extend it with ffmpeg invocation, output upload, retry/DLQ
// handling, and metrics.
type jobHandler struct {
	downloader *worker.Downloader
}

func (h *jobHandler) HandleJob(ctx context.Context, msg queue.JobMessage) error {
	path, err := h.downloader.DownloadSourceFromS3(ctx, msg.JobID, msg.SourceS3URI)
	if err != nil {
		return err
	}
	log.Printf("event=job_source_staged job_id=%s path=%s", msg.JobID, path)
	return nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
