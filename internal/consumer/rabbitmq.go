package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/samidala/event-processor/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Event struct {
	ID            string                 `json:"event_id"`
	CorrelationID string                 `json:"correlation_id"`
	Payload       map[string]interface{} `json:"payload"`
}

type Processor interface {
	Process(ctx context.Context, event Event) error
}

type RabbitMQConsumer struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	cfg       *config.Config
	processor Processor
	logger    *logrus.Entry
}

func NewRabbitMQConsumer(cfg *config.Config, processor Processor) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	err = ch.Qos(cfg.WorkerCount, 0, false) // Prefetch count matches worker count
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &RabbitMQConsumer{
		conn:      conn,
		ch:        ch,
		cfg:       cfg,
		processor: processor,
		logger:    logrus.WithField("component", "consumer"),
	}, nil
}

func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	msgs, err := c.ch.Consume(
		c.cfg.RabbitMQQueue,
		"event-processor",
		false, // manual-ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID, msgs)
		}(i)
	}

	<-ctx.Done()
	c.logger.Info("stopping consumer...")
	c.ch.Close()
	c.conn.Close()
	wg.Wait()
	return nil
}

func (c *RabbitMQConsumer) worker(ctx context.Context, id int, msgs <-chan amqp.Delivery) {
	workerLogger := c.logger.WithField("worker_id", id)
	workerLogger.Info("worker started")

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}

			var event Event
			if err := json.Unmarshal(d.Body, &event); err != nil {
				workerLogger.WithError(err).Error("failed to unmarshal event")
				d.Nack(false, false) // Don't requeue malformed messages
				continue
			}

			// Ensure metadata is available
			if event.ID == "" {
				workerLogger.Warn("event missing event_id, skipping")
				d.Ack(false)
				continue
			}

			err := c.processor.Process(ctx, event)
			if err != nil {
				workerLogger.WithError(err).WithFields(logrus.Fields{
					"event_id":       event.ID,
					"correlation_id": event.CorrelationID,
				}).Error("failed to process event")
				// Requeue if processing fails? Depending on error type.
				// For now, let's assume Process handles its own retries/DLQ.
				d.Nack(false, true)
				continue
			}

			d.Ack(false)
		}
	}
}
