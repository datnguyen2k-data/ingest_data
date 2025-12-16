package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"ingest_data/internal/config"
)

type OrderProducer struct {
	client *kgo.Client
	topic  string
}

func NewOrderProducer(cfg config.KafkaConfig) *OrderProducer {
	// Log cấu hình kết nối
	log.Printf("[Kafka Producer] Connecting to brokers: %v", cfg.Brokers)
	log.Printf("[Kafka Producer] Topic: %s", cfg.OrderTopic)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.OrderTopic),
		// Thêm các options để cải thiện reliability
		kgo.RequiredAcks(kgo.AllISRAcks()), // Đợi tất cả ISR confirm
		kgo.DisableIdempotentWrite(),       // Có thể bật nếu cần idempotent
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Printf("[Kafka Producer] Failed to create client: %v", err)
		panic(fmt.Errorf("create kafka producer: %w", err))
	}

	log.Printf("[Kafka Producer] Successfully created producer client")
	log.Printf("[Kafka Producer] Note: Connection will be tested on first publish")

	return &OrderProducer{
		client: client,
		topic:  cfg.OrderTopic,
	}
}

func (p *OrderProducer) PublishOrder(ctx context.Context, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("payload is empty")
	}

	rec := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(uuid.NewString()),
		Value:     payload,
		Timestamp: time.Now().UTC(),
	}

	// ProduceSync trả về slice lỗi, chỉ dùng 1 record nên lấy phần tử đầu
	results := p.client.ProduceSync(ctx, rec)

	if err := results.FirstErr(); err != nil {
		log.Printf("[Kafka Producer] Failed to publish to topic %s: %v", p.topic, err)
		log.Printf("[Kafka Producer] Payload size: %d bytes", len(payload))
		return fmt.Errorf("publish to kafka topic %s: %w", p.topic, err)
	}

	// Log thành công (có thể comment lại nếu quá nhiều log)
	// log.Printf("[Kafka Producer] Successfully published message to topic %s (size: %d bytes)", p.topic, len(payload))

	return nil
}

func (p *OrderProducer) Close(ctx context.Context) error {
	log.Printf("[Kafka Producer] Closing producer for topic %s", p.topic)
	p.client.Close()
	return nil
}
