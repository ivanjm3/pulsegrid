package pkg

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

// QueueDepthPollerConfig holds configuration for the Kafka queue depth poller.
type QueueDepthPollerConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string
	// Topic is the Kafka topic to measure depth for.
	Topic string
	// ConsumerGroup is the consumer group whose committed offsets define "consumed".
	ConsumerGroup string
	// PollInterval is how frequently to update the gauge. Default 30s.
	PollInterval time.Duration
	// Gauge is the Prometheus gauge to update.
	Gauge prometheus.Gauge
}

// StartQueueDepthPoller launches a background goroutine that queries Kafka admin API
// every PollInterval, calculates queue_depth = sum(partition end offset - group committed offset),
// and updates the Prometheus gauge. Stops when ctx is cancelled.
func StartQueueDepthPoller(ctx context.Context, cfg QueueDepthPollerConfig) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}

	go func() {
		ticker := time.NewTicker(cfg.PollInterval)
		defer ticker.Stop()

		// Poll once immediately on start.
		updateQueueDepth(ctx, cfg)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updateQueueDepth(ctx, cfg)
			}
		}
	}()
}

// updateQueueDepth fetches partition offsets from Kafka and calculates total lag.
func updateQueueDepth(ctx context.Context, cfg QueueDepthPollerConfig) {
	depth, err := fetchQueueDepth(ctx, cfg.Brokers, cfg.Topic, cfg.ConsumerGroup)
	if err != nil {
		log.Printf("WARN: queue depth poll failed: %v", err)
		return
	}
	cfg.Gauge.Set(float64(depth))
}

// fetchQueueDepth connects to Kafka, gets partition end offsets and consumer group committed offsets,
// returns sum of (end_offset - committed_offset) across all partitions.
func fetchQueueDepth(ctx context.Context, brokers []string, topic string, consumerGroup string) (int64, error) {
	// Use first reachable broker for leader lookup.
	var conn *kafka.Conn
	var err error
	for _, broker := range brokers {
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return 0, err
	}
	defer conn.Close()

	// Get partition list for topic.
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, err
	}

	var totalDepth int64

	for _, p := range partitions {
		leader := p.Leader
		leaderAddr := net.JoinHostPort(leader.Host, itoa(leader.Port))

		// Connect to partition leader to get end offset.
		pConn, err := kafka.DialLeader(ctx, "tcp", leaderAddr, topic, p.ID)
		if err != nil {
			log.Printf("WARN: queue depth: failed to dial leader for partition %d: %v", p.ID, err)
			continue
		}

		// Get latest (end) offset.
		endOffset, err := pConn.ReadLastOffset()
		if err != nil {
			pConn.Close()
			log.Printf("WARN: queue depth: failed to read end offset for partition %d: %v", p.ID, err)
			continue
		}

		pConn.Close()

		// Get consumer group committed offset for this partition.
		committedOffset := fetchCommittedOffset(ctx, brokers, topic, p.ID, consumerGroup)

		lag := endOffset - committedOffset
		if lag < 0 {
			lag = 0
		}
		totalDepth += lag
	}

	return totalDepth, nil
}

// fetchCommittedOffset gets the committed offset for a consumer group on a specific partition.
// Returns 0 if group has no committed offset (meaning all messages are unconsumed).
func fetchCommittedOffset(ctx context.Context, brokers []string, topic string, partition int, group string) int64 {
	client := &kafka.Client{
		Addr: kafka.TCP(brokers...),
	}

	resp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics: map[string][]int{
			topic: {partition},
		},
	})
	if err != nil {
		return 0
	}

	topicOffsets, ok := resp.Topics[topic]
	if !ok {
		return 0
	}

	for _, po := range topicOffsets {
		if po.Partition == partition {
			if po.CommittedOffset < 0 {
				return 0
			}
			return po.CommittedOffset
		}
	}

	return 0
}

// itoa converts int to string without importing strconv (small helper).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 5)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
