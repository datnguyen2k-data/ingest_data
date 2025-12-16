package main

import (
	"context"
	"log"
	"time"

	app "ingest_data/internal/application/pancake"
	"ingest_data/internal/config"
	httpPancake "ingest_data/internal/infrastructure/http/pancake"
	kafkaInfra "ingest_data/internal/infrastructure/messaging/kafka"
)

// Chương trình nhỏ để fetch orders từ Pancake và đẩy vào Kafka topic pancake_chando_sale_order.
// Mặc định lấy trong 1 ngày gần nhất (24h).
func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	// Validate Kafka config
	if cfg.Kafka.OrderTopic == "" {
		log.Fatal("KAFKA_ORDER_TOPIC is empty (ví dụ: pancake_chando_sale_order)")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS is empty (ví dụ: localhost:19092,localhost:29092,localhost:39092)")
	}

	log.Printf("[Config] Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("[Config] Kafka topic: %s", cfg.Kafka.OrderTopic)
	log.Printf("[Config] Pancake Shop ID: %s", cfg.Pancake.ShopID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := httpPancake.NewClient(cfg.Pancake)

	log.Printf("[Kafka] Initializing producer...")
	producer := kafkaInfra.NewOrderProducer(cfg.Kafka)
	defer func() {
		log.Printf("[Kafka] Closing producer...")
		producer.Close(ctx)
	}()

	svc := app.NewService(client, producer)

	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour) // bạn có thể chỉnh lại logic time ở đây

	log.Printf("[Main] Fetching Pancake orders from %s to %s ...", start.Format(time.RFC3339), end.Format(time.RFC3339))

	n, err := svc.SyncIncremental(ctx, &start, &end)
	if err != nil {
		log.Fatalf("[Main] Sync incremental failed: %v", err)
	}

	log.Printf("[Main] Successfully synced %d orders to Kafka topic %s", n, cfg.Kafka.OrderTopic)
}
