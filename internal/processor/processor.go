package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/samidala/event-processor/internal/config"
	"github.com/samidala/event-processor/internal/consumer"
	"github.com/samidala/event-processor/internal/idempotency"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
)

type KafkaProducer interface {
	Publish(ctx context.Context, eventID, correlationID string, payload interface{}) error
}

type EventProcessor struct {
	producer KafkaProducer
	store    idempotency.Store
	cb       *gobreaker.CircuitBreaker
	logger   *logrus.Entry
	cfg      *config.Config
}

func NewEventProcessor(cfg *config.Config, producer KafkaProducer, store idempotency.Store) *EventProcessor {
	st := gobreaker.Settings{
		Name:        "kafka-producer",
		MaxRequests: 5,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
	}

	return &EventProcessor{
		producer: producer,
		store:    store,
		cb:       gobreaker.NewCircuitBreaker(st),
		logger:   logrus.WithField("component", "processor"),
		cfg:      cfg,
	}
}

func (p *EventProcessor) Process(ctx context.Context, event consumer.Event) error {
	log := p.logger.WithFields(logrus.Fields{
		"event_id":       event.ID,
		"correlation_id": event.CorrelationID,
	})

	// 1. Idempotency Check
	processed, err := p.store.IsProcessed(ctx, event.ID)
	if err != nil {
		log.WithError(err).Error("idempotency check failed")
		return err // Retryable? Probably.
	}
	if processed {
		log.Info("event already processed, skipping")
		return nil
	}

	// 2. Publish with Circuit Breaker and Retries
	err = retry.Do(
		func() error {
			_, cbErr := p.cb.Execute(func() (interface{}, error) {
				return nil, p.producer.Publish(ctx, event.ID, event.CorrelationID, event.Payload)
			})
			return cbErr
		},
		retry.Context(ctx),
		retry.Attempts(uint(p.cfg.MaxRetries)),
		retry.Delay(p.cfg.RetryInitialDelay),
		retry.MaxDelay(p.cfg.RetryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.Warnf("retrying publish (attempt %d): %v", n+1, err)
		}),
	)

	if err != nil {
		log.WithError(err).Error("all retries failed, sending to DLQ")
		// TODO: Implementation of DLQ publish
		return fmt.Errorf("failed after retries: %w", err)
	}

	// 3. Mark as Processed
	if err := p.store.MarkProcessed(ctx, event.ID, 24*time.Hour); err != nil {
		log.WithError(err).Error("failed to mark event as processed")
		// This is tricky: we published to Kafka but failed to mark in Redis.
		// If we return error, RabbitMQ will requeue and we will publish again.
		// BUT the producer is idempotent (same Key), so it should be fine.
		return err
	}

	log.Info("event processed successfully")
	return nil
}
