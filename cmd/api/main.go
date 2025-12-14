package main

import (
	"log"

	"ingest_data/internal/application/order"
	"ingest_data/internal/config"
	"ingest_data/internal/infrastructure/http/gin"
	"ingest_data/internal/infrastructure/persistence/postgres"
)

func main() {
	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	// 2. Init Postgres connection pool
	pool, err := postgres.NewPool(cfg.Postgres)
	if err != nil {
		log.Fatalf("postgres connection failed: %v", err)
	}
	defer pool.Close()

	// 3. Init repositories (infrastructure)
	orderRepo := postgres.NewOrderRepository(pool)

	// 4. Init application services (use cases)
	orderService := order.NewService(orderRepo)

	// 5. Init HTTP server (interface layer)
	server := gin.NewServer(
		cfg.Server,
		orderService,
	)

	// 6. Start server
	if err := server.Run(); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
