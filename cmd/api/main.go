package main

import (
	"context"

	"ingest_data/internal/application/order"
	"ingest_data/internal/config"
	ginserver "ingest_data/internal/infrastructure/http/gin"
	kafkainfra "ingest_data/internal/infrastructure/messaging/kafka"
	"ingest_data/internal/infrastructure/persistence/postgres"
	"ingest_data/internal/interfaces/http/handler"
	"ingest_data/internal/interfaces/http/router"
	"ingest_data/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic("load config failed: " + err.Error())
	}

	// Khởi tạo logger với DI
	log, err := logger.NewZapLogger(cfg.App.Env)
	if err != nil {
		panic("initialize logger failed: " + err.Error())
	}
	defer log.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := postgres.NewPool(cfg.DB)
	if err != nil {
		log.Fatal("Postgres connection failed", logger.Error(err))
	}
	defer pool.Close()

	orderRepo := postgres.NewOrderRepository(pool)

	// Inject logger vào producer
	producer := kafkainfra.NewOrderProducer(cfg.Kafka, log)
	defer func() {
		if err := producer.Close(ctx); err != nil {
			log.Error("Failed to close Kafka producer", logger.Error(err))
		}
	}()

	orderService := order.NewService(orderRepo, producer)

	consumer := kafkainfra.NewOrderConsumer(cfg.Kafka, orderService)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Error("Kafka consumer stopped", logger.Error(err))
		}
	}()
	defer consumer.Close()

	orderHandler := handler.NewOrderHandler(orderService)
	engine := ginserver.NewEngine()
	router.RegisterRoutes(engine, orderHandler)

	server := ginserver.NewServer(cfg.Server, engine)
	log.Info("Starting HTTP server",
		logger.String("address", cfg.Server.Address()))
	if err := server.Run(); err != nil {
		log.Fatal("Server run failed", logger.Error(err))
	}
}

