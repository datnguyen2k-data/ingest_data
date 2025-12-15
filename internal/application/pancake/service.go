package pancake

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// OrderFetcher abstract client Pancake để dễ test.
type OrderFetcher interface {
	FetchOrdersIncremental(ctx context.Context, startDate, endDate *time.Time) ([]json.RawMessage, error)
}

// Publisher dùng lại interface giống application/order.
type Publisher interface {
	PublishOrder(ctx context.Context, payload []byte) error
}

type Service struct {
	fetcher   OrderFetcher
	publisher Publisher
}

func NewService(fetcher OrderFetcher, publisher Publisher) *Service {
	return &Service{
		fetcher:   fetcher,
		publisher: publisher,
	}
}

// SyncIncremental fetch orders Pancake và đẩy từng order (raw JSON) vào Kafka.
func (s *Service) SyncIncremental(
	ctx context.Context,
	startDate *time.Time,
	endDate *time.Time,
) (int, error) {
	orders, err := s.fetcher.FetchOrdersIncremental(ctx, startDate, endDate)
	if err != nil {
		return 0, fmt.Errorf("fetch orders: %w", err)
	}

	count := 0
	for _, raw := range orders {
		// đảm bảo raw là JSON hợp lệ
		if !json.Valid(raw) {
			continue
		}
		if err := s.publisher.PublishOrder(ctx, raw); err != nil {
			return count, fmt.Errorf("publish order #%d: %w", count, err)
		}
		count++
	}
	return count, nil
}
