package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsConsumedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_consumed_total",
		Help: "The total number of events consumed from RabbitMQ",
	})

	EventsPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_published_total",
		Help: "The total number of events published to Kafka",
	})

	RetryAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "retry_attempts_total",
		Help: "The total number of retry attempts",
	})

	DeadLetterTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dead_letter_total",
		Help: "The total number of events sent to dead letter queue",
	})

	ProcessingDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "processing_duration_seconds",
		Help:    "Duration of event processing",
		Buckets: prometheus.DefBuckets,
	})

	InFlightEvents = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "in_flight_events",
		Help: "The number of events currently being processed",
	})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "State of the circuit breaker (0: closed, 1: open, 2: half-open)",
	}, []string{"name"})
)
