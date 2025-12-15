package postgres

import (
	"context"
	"time"

	"ingest_data/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	println("User:", cfg.User)
    println("DBName:", cfg.DBName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, err
	}

	poolCfg.MaxConns = int32(cfg.MaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}

	return pool, nil
}



