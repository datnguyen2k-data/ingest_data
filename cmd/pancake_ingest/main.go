package main

import (
	"context"
	"time"

	app "ingest_data/internal/application/pancake"
	"ingest_data/internal/config"
	httpPancake "ingest_data/internal/infrastructure/http/pancake"
	kafkaInfra "ingest_data/internal/infrastructure/messaging/kafka"
	"ingest_data/pkg/logger"
)

// Chương trình nhỏ để fetch orders từ Pancake và đẩy vào Kafka topic pancake_chando_sale_order.
// Mặc định lấy trong 1 ngày gần nhất (24h).
func main() {
	cfg, err := config.Load()
	if err != nil {
		// Fallback logger nếu chưa khởi tạo được
		panic("load config failed: " + err.Error())
	}

	// Khởi tạo logger với DI
	log, err := logger.NewZapLogger(cfg.App.Env)
	if err != nil {
		panic("initialize logger failed: " + err.Error())
	}
	defer log.Sync()

	// Validate Kafka config
	if cfg.Kafka.OrderTopic == "" {
		log.Fatal("KAFKA_ORDER_TOPIC is empty (ví dụ: pancake_chando_sale_order)")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS is empty (ví dụ: localhost:19092,localhost:29092,localhost:39092)")
	}

	log.Info("Starting Pancake ingestion service",
		logger.String("app_name", cfg.App.Name),
		logger.String("env", cfg.App.Env),
		logger.Any("kafka_brokers", cfg.Kafka.Brokers),
		logger.String("kafka_topic", cfg.Kafka.OrderTopic),
		logger.String("pancake_shop_id", cfg.Pancake.ShopID))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Inject logger vào các dependencies (DI)
	client := httpPancake.NewClient(cfg.Pancake, log)
	producer := kafkaInfra.NewOrderProducer(cfg.Kafka, log)
	defer func() {
		if err := producer.Close(ctx); err != nil {
			log.Error("Failed to close Kafka producer", logger.Error(err))
		}
	}()

	svc := app.NewService(client, producer)

	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour) // bạn có thể chỉnh lại logic time ở đây

	log.Info("Fetching Pancake orders",
		logger.String("start_time", start.Format(time.RFC3339)),
		logger.String("end_time", end.Format(time.RFC3339)))

	n, err := svc.SyncIncremental(ctx, &start, &end)
	if err != nil {
		log.Fatal("Sync incremental failed",
			logger.Error(err),
			logger.Int("synced_count", n))
	}

	log.Info("Successfully synced orders to Kafka",
		logger.Int("total_orders", n),
		logger.String("topic", cfg.Kafka.OrderTopic))
}
