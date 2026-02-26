# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o event-processor .

# Final stage
FROM alpine:3.18

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/event-processor .

EXPOSE 8080

CMD ["./event-processor"]
