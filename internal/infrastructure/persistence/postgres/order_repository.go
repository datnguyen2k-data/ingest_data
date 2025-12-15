package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	domain "ingest_data/internal/domain/order"
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

func (r *OrderRepository) Save(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("order is nil")
	}

	const query = `
		INSERT INTO orders (id, customer_id, product_id, quantity, price, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE
		SET customer_id = EXCLUDED.customer_id,
			product_id = EXCLUDED.product_id,
			quantity = EXCLUDED.quantity,
			price = EXCLUDED.price,
			status = EXCLUDED.status,
			created_at = EXCLUDED.created_at;
	`

	if err := r.ensureTable(ctx); err != nil {
		return err
	}

	_, err := r.pool.Exec(ctx, query,
		order.ID,
		order.CustomerID,
		order.ProductID,
		order.Quantity,
		order.Price,
		order.Status,
		order.CreatedAt,
	)
	return err
}

func (r *OrderRepository) FindByID(ctx context.Context, id string) (*domain.Order, error) {
	const query = `
		SELECT id, customer_id, product_id, quantity, price, status, created_at
		FROM orders
		WHERE id = $1;
	`
	var o domain.Order
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&o.ID,
		&o.CustomerID,
		&o.ProductID,
		&o.Quantity,
		&o.Price,
		&o.Status,
		&o.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *OrderRepository) ensureTable(ctx context.Context) error {
	const stmt = `
		CREATE TABLE IF NOT EXISTS orders (
			id TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			product_id TEXT NOT NULL,
			quantity INT NOT NULL,
			price NUMERIC NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
	`
	_, err := r.pool.Exec(ctx, stmt)
	return err
}
