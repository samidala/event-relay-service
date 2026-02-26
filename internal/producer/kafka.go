package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samidala/event-processor/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	writer *kafka.Writer
	logger *logrus.Entry
}

func NewKafkaProducer(cfg *config.Config) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll, // acks=all
		MaxAttempts:            cfg.MaxRetries,
		AllowAutoTopicCreation: true,
	}

	return &KafkaProducer{
		writer: writer,
		logger: logrus.WithField("component", "producer"),
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, eventID, correlationID string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(eventID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "correlation_id", Value: []byte(correlationID)},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
