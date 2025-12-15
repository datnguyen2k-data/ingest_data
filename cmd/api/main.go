package main

import (
	"context"
	"log"

	"ingest_data/internal/application/order"
	"ingest_data/internal/config"
	ginserver "ingest_data/internal/infrastructure/http/gin"
	kafkainfra "ingest_data/internal/infrastructure/messaging/kafka"
	"ingest_data/internal/infrastructure/persistence/postgres"
	"ingest_data/internal/interfaces/http/handler"
	"ingest_data/internal/interfaces/http/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := postgres.NewPool(cfg.DB)
	if err != nil {
		log.Fatalf("postgres connection failed: %v", err)
	}
	defer pool.Close()

	orderRepo := postgres.NewOrderRepository(pool)

	producer := kafkainfra.NewOrderProducer(cfg.Kafka)
	defer producer.Close(ctx)

	orderService := order.NewService(orderRepo, producer)

	consumer := kafkainfra.NewOrderConsumer(cfg.Kafka, orderService)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	}()
	defer consumer.Close()

	orderHandler := handler.NewOrderHandler(orderService)
	engine := ginserver.NewEngine()
	router.RegisterRoutes(engine, orderHandler)

	server := ginserver.NewServer(cfg.Server, engine)
	if err := server.Run(); err != nil {
		log.Fatalf("server run failed: %v", err)
	}
}

// func main() {
// 	cfg, err := config.Load()
// 	if err != nil {
// 		log.Fatalf("load config failed: %v", err)
// 	}
// 	log.Printf("config: %+v", cfg)
// }
