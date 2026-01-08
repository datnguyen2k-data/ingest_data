package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"
)

type OrderProducer struct {
	client *kgo.Client
	topic  string
	logger logger.Logger
}

// NewOrderProducer tạo producer mới với logger được inject (DI)
func NewOrderProducer(cfg config.KafkaConfig, log logger.Logger) *OrderProducer {
	// Log cấu hình kết nối
	log.Info("Connecting to Kafka brokers",
		logger.Any("brokers", cfg.Brokers),
		logger.String("topic", cfg.OrderTopic))

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.OrderTopic),
		// Thêm các options để cải thiện reliability
		kgo.RequiredAcks(kgo.AllISRAcks()), // Đợi tất cả ISR confirm
		kgo.DisableIdempotentWrite(),       // Có thể bật nếu cần idempotent
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("Failed to create Kafka producer client", logger.Error(err))
		panic(fmt.Errorf("create kafka producer: %w", err))
	}

	log.Info("Successfully created Kafka producer client",
		logger.String("topic", cfg.OrderTopic),
		logger.String("note", "Connection will be tested on first publish"))

	return &OrderProducer{
		client: client,
		topic:  cfg.OrderTopic,
		logger: log,
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
		p.logger.Error("Failed to publish message to Kafka",
			logger.String("topic", p.topic),
			logger.Int("payload_size", len(payload)),
			logger.Error(err))
		return fmt.Errorf("publish to kafka topic %s: %w", p.topic, err)
	}

	// Log thành công ở debug level để tránh quá nhiều log
	p.logger.Debug("Successfully published message to Kafka",
		logger.String("topic", p.topic),
		logger.Int("payload_size", len(payload)))

	return nil
}

func (p *OrderProducer) Close(ctx context.Context) error {
	p.logger.Info("Closing Kafka producer", logger.String("topic", p.topic))
	p.client.Close()
	return nil
}
