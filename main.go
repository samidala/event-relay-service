package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samidala/event-processor/internal/config"
	"github.com/samidala/event-processor/internal/consumer"
	"github.com/samidala/event-processor/internal/idempotency"
	"github.com/samidala/event-processor/internal/processor"
	"github.com/samidala/event-processor/internal/producer"
	"github.com/samidala/event-processor/internal/tracing"
	"github.com/sirupsen/logrus"
)

func main() {
	// Initialize logger
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logrus.Fatalf("failed to load config: %v", err)
	}

	// Initialize tracing
	tp, err := tracing.InitTracer("event-processor", os.Getenv("JAEGER_URL"))
	if err != nil {
		logrus.Warnf("failed to initialize tracer: %v", err)
	} else {
		defer func() { _ = tp.Shutdown(context.Background()) }()
	}

	// Initialize store
	store := idempotency.NewRedisStore(cfg.RedisAddr, cfg.RedisPassword)

	// Initialize producer
	kafkaProducer := producer.NewKafkaProducer(cfg)
	defer kafkaProducer.Close()

	// Initialize processor
	eventProcessor := processor.NewEventProcessor(cfg, kafkaProducer, store)

	// Initialize consumer
	rmqConsumer, err := consumer.NewRabbitMQConsumer(cfg, eventProcessor)
	if err != nil {
		logrus.Fatalf("failed to create consumer: %v", err)
	}

	// Start health and metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})
		logrus.Infof("starting health/metrics server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			logrus.Errorf("failed to start health/metrics server: %v", err)
		}
	}()

	// Graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logrus.Info("starting event processor service...")
	if err := rmqConsumer.Start(ctx); err != nil {
		logrus.Errorf("consumer failure: %v", err)
	}

	logrus.Info("service shutdown complete")
}
