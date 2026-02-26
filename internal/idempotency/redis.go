package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Store interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string, ttl time.Duration) error
}

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr, password string) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &RedisStore{client: client}
}

func (s *RedisStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	key := fmt.Sprintf("event:%s", eventID)
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func (s *RedisStore) MarkProcessed(ctx context.Context, eventID string, ttl time.Duration) error {
	key := fmt.Sprintf("event:%s", eventID)
	return s.client.Set(ctx, key, "1", ttl).Err()
}
