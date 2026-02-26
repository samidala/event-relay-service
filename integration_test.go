package main

import (
	"context"
	"testing"
	"time"

	"github.com/samidala/event-processor/internal/config"
	"github.com/samidala/event-processor/internal/consumer"
	"github.com/samidala/event-processor/internal/idempotency"
	"github.com/samidala/event-processor/internal/processor"
	"github.com/samidala/event-processor/internal/producer"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestIntegration_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Spin up Redis
	redisContainer, err := redis.RunContainer(ctx)
	assert.NoError(t, err)
	defer redisContainer.Terminate(ctx)
	redisAddr, _ := redisContainer.ConnectionString(ctx)

	// Spin up Kafka
	kafkaContainer, err := kafka.RunContainer(ctx)
	assert.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)
	kafkaBrokers, _ := kafkaContainer.Brokers(ctx)

	cfg := &config.Config{
		KafkaBrokers:      kafkaBrokers,
		KafkaTopic:        "test-topic",
		RedisAddr:         redisAddr,
		MaxRetries:        3,
		RetryInitialDelay: 10 * time.Millisecond,
	}

	store := idempotency.NewRedisStore(cfg.RedisAddr, "")
	kafkaProducer := producer.NewKafkaProducer(cfg)
	defer kafkaProducer.Close()

	p := processor.NewEventProcessor(cfg, kafkaProducer, store)

	t.Run("Process event end-to-end", func(t *testing.T) {
		event := consumer.Event{
			ID:            "evt-123",
			CorrelationID: "corr-456",
			Payload:       map[string]interface{}{"message": "hello integration"},
		}

		err := p.Process(ctx, event)
		assert.NoError(t, err)

		// Verify idempotency
		processed, err := store.IsProcessed(ctx, "evt-123")
		assert.NoError(t, err)
		assert.True(t, processed)

		// Second process should be skipped
		err = p.Process(ctx, event)
		assert.NoError(t, err)
	})
}
