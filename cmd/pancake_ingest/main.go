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

	// đảm bảo topic trong env là pancake_chando_sale_order
	if cfg.Kafka.OrderTopic == "" {
		log.Fatal("KAFKA_ORDER_TOPIC is empty (ví dụ: pancake_chando_sale_order)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := httpPancake.NewClient(cfg.Pancake)
	producer := kafkaInfra.NewOrderProducer(cfg.Kafka)
	defer producer.Close(ctx)

	svc := app.NewService(client, producer)

	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour) // bạn có thể chỉnh lại logic time ở đây
    
	log.Printf("fetching Pancake orders from %s to %s ...", start.Format(time.RFC3339), end.Format(time.RFC3339))
	n, err := svc.SyncIncremental(ctx, &start, &end)
	if err != nil {
		log.Fatalf("sync incremental failed: %v", err)
	}

	log.Printf("synced %d orders to Kafka topic %s", n, cfg.Kafka.OrderTopic)
}
