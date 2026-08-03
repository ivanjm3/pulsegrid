// Command analytics-consumer runs the Pulsegrid analytics consumer: it
// consumes job lifecycle events from Kafka and sinks them into
// analytics.job_lifecycle_events (task 37).
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"pulsegrid/pkg/analytics"
	"pulsegrid/pkg/queue"
	"pulsegrid/pkg/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	brokers := strings.Split(envOrDefault("ANALYTICS_KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := envOrDefault("ANALYTICS_CONSUMER_GROUP", analytics.GroupID)
	topic := envOrDefault("LIFECYCLE_TOPIC", queue.LifecycleTopic)
	dbDSN := os.Getenv("ANALYTICS_DB_DSN")

	// Run migrations here too (not just from cmd/api): the analytics
	// schema (migration 003) must exist before the sink can insert into
	// it, and this consumer may start before, after, or independently of
	// the API server. RunMigrations is idempotent (migrate.ErrNoChange is
	// treated as success), so running it from both entrypoints is safe.
	if err := store.RunMigrations(dbDSN); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	pool, err := store.Connect(ctx, dbDSN)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	reader := queue.NewKafkaLifecycleReader(brokers, groupID, topic)
	sink := analytics.NewPostgresSink(pool)
	consumer := analytics.NewConsumer(reader, sink)

	refresher := analytics.NewRefresher(pool)
	go refresher.RunLoop(ctx)

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
