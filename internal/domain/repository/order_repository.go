package repository

import (
	"context"

	"ingest_data/internal/domain/order"
)

type OrderRepository interface {
	Save(ctx context.Context, order *order.Order) error
	FindByID(ctx context.Context, id string) (*order.Order, error)
}
