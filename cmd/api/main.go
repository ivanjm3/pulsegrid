// Command api runs the Pulsegrid API server.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"pulsegrid/pkg/api"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/storage"
	"pulsegrid/pkg/store"
)

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

	uploadHandler := api.NewUploadHandler(uploader, producer, db, outputBucket)
	manifests := storage.NewDownloader(s3Client, outputBucket)
	statusHandler := api.NewStatusHandler(db, manifests)

	mux := http.NewServeMux()
	mux.Handle("/videos/upload", uploadHandler)
	mux.Handle("GET /jobs/{job_id}", statusHandler)

	const addr = ":8080"
	log.Printf("pulsegrid api server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
