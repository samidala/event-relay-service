# Event Processor Service (RabbitMQ to Kafka)

A production-grade Go service that consumes events from RabbitMQ and publishes them to Kafka with at-least-once delivery, idempotency, and high observability.

## Architecture Design

The service follows a concurrent, worker-pool-based architecture designed for high throughput and resiliency.

### Core Components:
- **RabbitMQ Consumer**: Uses manual acknowledgment mode. Messages are only ACKed after a successful Kafka publish and idempotency check.
- **Idempotency Store (Redis)**: Before processing, each `event_id` is checked against Redis. If already processed, it's skipped. After a successful publish, it's marked as processed.
- **Idempotent Kafka Producer**: Configurable with `acks=all` and internal retries. Uses `event_id` as the message key to ensure partition-level ordering and producer-side idempotency.
- **Resiliency Layer**:
  - **Circuit Breaker**: Prevents cascading failures when Kafka is down.
  - **Exponential Backoff**: Retries transient errors with increasing delays and jitter.
  - **Worker Pool**: Limits concurrent processing and provides backpressure.

```mermaid
graph LR
    RMQ[RabbitMQ] --> Consumer[RMQ Consumer]
    Consumer --> WP[Worker Pool]
    WP --> Processor[Event Processor]
    Processor --> Redis[(Redis Idempotency)]
    Processor --> Kafka[Kafka Producer]
    Kafka --> KTopic[Kafka Topic]
```

## Getting Started

### Prerequisites:
- Go 1.21+
- Docker & Docker Compose
- RabbitMQ, Kafka, and Redis instances

### Environment Variables:
| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_URL` | RabbitMQ connection string | `amqp://guest:guest@localhost:5672/` |
| `RABBITMQ_QUEUE` | Target RabbitMQ queue | `events` |
| `KAFKA_BROKERS` | Comma-separated Kafka brokers | `localhost:9092` |
| `KAFKA_TOPIC` | Target Kafka topic | `processed_events` |
| `REDIS_ADDR` | Redis address | `localhost:6379` |
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |

### Running Locally:
1. Start infrastructure (e.g., via Docker Compose - not included in code).
2. Run the service:
   ```bash
   go run main.go
   ```

## Testing

### Unit Tests:
Focus on the processor logic, retries, and idempotency checks.
```bash
go test -v ./internal/processor/...
```

### Integration Tests:
Uses `testcontainers-go` to spin up real dependencies (Docker required).
```bash
go test -v ./integration_test.go
```

## Deployment

### Containerization:
The project includes a multi-stage `Dockerfile` for a minimal production image.
```bash
docker build -t event-processor:latest .
```

### Kubernetes:
A Helm chart is provided in `deploy/helm/event-processor`.
```bash
helm install event-processor ./deploy/helm/event-processor
```

### Infrastructure (IaC):
Terraform modules are available for:
- **AWS**: VPC, IAM, and EKS cluster.
- **GCP**: VPC and GKE cluster.

Refer to `terraform/aws/main.tf` and `terraform/gcp/main.tf` for details.

## Observability
- **Metrics**: Prometheus metrics exposed at `/metrics` (Port 8080).
- **Tracing**: OpenTelemetry (Jaeger) support for distributed tracing.
- **Health**: Liveness and readiness checks at `/health`.
