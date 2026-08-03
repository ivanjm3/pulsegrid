// Command analytics-consumer runs the Pulsegrid analytics consumer: it
// consumes job lifecycle events from Kafka and sinks them for the analytics
// pipeline (task 36). Sinking to Postgres lands in a later task; today this
// scaffold logs each event and gates the offset commit on a successful sink
// write, so a future sink outage never silently drops an event.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"pulsegrid/pkg/analytics"
	"pulsegrid/pkg/queue"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	brokers := strings.Split(envOrDefault("ANALYTICS_KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := envOrDefault("ANALYTICS_CONSUMER_GROUP", analytics.GroupID)
	topic := envOrDefault("LIFECYCLE_TOPIC", queue.LifecycleTopic)
	_ = os.Getenv("ANALYTICS_DB_DSN") // wired to the Postgres sink in task 37

	reader := queue.NewKafkaLifecycleReader(brokers, groupID, topic)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	handler := analytics.NewLogEventHandler(logger)
	consumer := analytics.NewConsumer(reader, handler)

	log.Printf("pulsegrid analytics-consumer starting: brokers=%v group=%s topic=%s", brokers, groupID, topic)
	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("consumer exited: %v", err)
	}
	log.Print("pulsegrid analytics-consumer shut down cleanly")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
