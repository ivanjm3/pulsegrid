// Command api runs the Pulsegrid API server.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"pulsegrid/pkg/api"
	"pulsegrid/pkg/metrics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/storage"
	"pulsegrid/pkg/store"
)

// queueDepthPollInterval is how often the /metrics queue depth gauge is
// refreshed from the Kafka admin API.
const queueDepthPollInterval = 30 * time.Second

func main() {
	ctx := context.Background()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)
	sourceBucket := envOrDefault("S3_BUCKET_SOURCE", "pulsegrid-source")
	outputBucket := envOrDefault("S3_BUCKET_OUTPUT", "pulsegrid-output")
	uploader := storage.NewUploader(s3Client, sourceBucket)

	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	writer := queue.NewKafkaWriter(brokers)
	producer := queue.NewProducer(writer)
	defer producer.Close()

	pool, err := store.Connect(ctx, os.Getenv("DB_DSN"))
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()
	db := store.NewStore(pool)

	m := metrics.New()

	uploadHandler := api.NewUploadHandler(uploader, producer, db, outputBucket, m)
	manifests := storage.NewDownloader(s3Client, outputBucket)
	statusHandler := api.NewStatusHandler(db, manifests)
	jobsListHandler := api.NewJobsListHandler(db)

	kafkaPinger := &queue.Pinger{Brokers: brokers}
	bucketPinger := storage.NewBucketPinger(s3Client, sourceBucket)
	healthHandler := api.NewHealthHandler(kafkaPinger, pool, bucketPinger)

	go pollQueueDepth(ctx, brokers, m)

	mux := http.NewServeMux()
	mux.Handle("/videos/upload", uploadHandler)
	mux.Handle("GET /jobs/{job_id}", statusHandler)
	mux.Handle("GET /jobs", jobsListHandler)
	mux.Handle("GET /health", healthHandler)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", m.Handler())
	go func() {
		const metricsAddr = ":8081"
		log.Printf("pulsegrid api metrics listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, metricsMux); err != nil {
			log.Fatal(err)
		}
	}()

	const addr = ":8080"
	log.Printf("pulsegrid api server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

// pollQueueDepth refreshes the queue depth gauge from the Kafka admin API
// every queueDepthPollInterval. It runs until ctx is cancelled.
func pollQueueDepth(ctx context.Context, brokers []string, m *metrics.Metrics) {
	ticker := time.NewTicker(queueDepthPollInterval)
	defer ticker.Stop()
	for {
		depth, err := queue.QueueDepth(ctx, brokers)
		if err != nil {
			log.Printf("event=queue_depth_poll_failed error=%v", err)
		} else {
			m.SetQueueDepth(float64(depth))
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
