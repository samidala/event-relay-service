package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/samidala/event-processor/internal/config"
	"github.com/samidala/event-processor/internal/consumer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Publish(ctx context.Context, eventID, correlationID string, payload interface{}) error {
	args := m.Called(ctx, eventID, correlationID, payload)
	return args.Error(0)
}

type MockStore struct {
	mock.Mock
}

func (m *MockStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	args := m.Called(ctx, eventID)
	return args.Bool(0), args.Error(1)
}

func (m *MockStore) MarkProcessed(ctx context.Context, eventID string, ttl time.Duration) error {
	args := m.Called(ctx, eventID, ttl)
	return args.Error(0)
}

func TestEventProcessor_Process(t *testing.T) {
	cfg := &config.Config{
		MaxRetries:        1,
		RetryInitialDelay: 1 * time.Millisecond,
	}

	t.Run("successful processing", func(t *testing.T) {
		producer := new(MockProducer)
		store := new(MockStore)
		p := NewEventProcessor(cfg, producer, store)

		event := consumer.Event{ID: "1", CorrelationID: "c1", Payload: map[string]interface{}{"foo": "bar"}}

		store.On("IsProcessed", mock.Anything, "1").Return(false, nil)
		producer.On("Publish", mock.Anything, "1", "c1", event.Payload).Return(nil)
		store.On("MarkProcessed", mock.Anything, "1", mock.Anything).Return(nil)

		err := p.Process(context.Background(), event)

		assert.NoError(t, err)
		store.AssertExpectations(t)
		producer.AssertExpectations(t)
	})

	t.Run("skip already processed", func(t *testing.T) {
		producer := new(MockProducer)
		store := new(MockStore)
		p := NewEventProcessor(cfg, producer, store)

		event := consumer.Event{ID: "1", CorrelationID: "c1"}

		store.On("IsProcessed", mock.Anything, "1").Return(true, nil)

		err := p.Process(context.Background(), event)

		assert.NoError(t, err)
		producer.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("retry and succeed", func(t *testing.T) {
		producer := new(MockProducer)
		store := new(MockStore)
		p := NewEventProcessor(cfg, producer, store)

		event := consumer.Event{ID: "1", CorrelationID: "c1"}

		store.On("IsProcessed", mock.Anything, "1").Return(false, nil)
		producer.On("Publish", mock.Anything, "1", "c1", event.Payload).Return(errors.New("kafka error")).Once()
		producer.On("Publish", mock.Anything, "1", "c1", event.Payload).Return(nil).Once()
		store.On("MarkProcessed", mock.Anything, "1", mock.Anything).Return(nil)

		err := p.Process(context.Background(), event)

		assert.NoError(t, err)
	})
}
