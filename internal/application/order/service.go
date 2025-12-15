package order

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	domain "ingest_data/internal/domain/order"
	"ingest_data/internal/domain/repository"
)

type Publisher interface {
	PublishOrder(ctx context.Context, payload []byte) error
}

type Service struct {
	repo      repository.OrderRepository
	publisher Publisher
}

type CreateOrderCommand struct {
	CustomerID string  `json:"customer_id"`
	ProductID  string  `json:"product_id"`
	Quantity   int     `json:"quantity"`
	Price      float64 `json:"price"`
}

func NewService(repo repository.OrderRepository, publisher Publisher) *Service {
	return &Service{repo: repo, publisher: publisher}
}

// SubmitOrder nhận request từ API và đẩy vào Kafka (write-only path).
func (s *Service) SubmitOrder(ctx context.Context, cmd CreateOrderCommand) (*domain.Order, error) {
	id := uuid.NewString()
	order, err := domain.NewOrder(id, cmd.CustomerID, cmd.ProductID, cmd.Quantity, cmd.Price)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("encode order: %w", err)
	}

	if err := s.publisher.PublishOrder(ctx, data); err != nil {
		return nil, fmt.Errorf("publish order: %w", err)
	}

	return order, nil
}

// HandleConsumedOrder lưu order đã được consumer đọc từ Kafka vào Postgres.
func (s *Service) HandleConsumedOrder(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("order is nil")
	}
	if err := s.repo.Save(ctx, order); err != nil {
		return fmt.Errorf("save order: %w", err)
	}
	return nil
}
