package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	RabbitMQURL       string
	RabbitMQQueue     string
	KafkaBrokers      []string
	KafkaTopic        string
	RedisAddr         string
	RedisPassword     string
	LogLevel          string
	WorkerCount       int
	MaxRetries        int
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	CircuitBreakerThreshold int
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load() // Ignore error if .env doesn't exist

	return &Config{
		RabbitMQURL:             getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQQueue:           getEnv("RABBITMQ_QUEUE", "events"),
		KafkaBrokers:            []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopic:              getEnv("KAFKA_TOPIC", "processed_events"),
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:           getEnv("REDIS_PASSWORD", ""),
		LogLevel:                getEnv("LOG_LEVEL", "info"),
		WorkerCount:             getEnvInt("WORKER_COUNT", 10),
		MaxRetries:              getEnvInt("MAX_RETRIES", 5),
		RetryInitialDelay:       time.Duration(getEnvInt("RETRY_INITIAL_DELAY_MS", 100)) * time.Millisecond,
		RetryMaxDelay:           time.Duration(getEnvInt("RETRY_MAX_DELAY_MS", 5000)) * time.Millisecond,
		CircuitBreakerThreshold: getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 5),
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}
