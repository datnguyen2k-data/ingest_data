package kafka

import (
	"context"
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
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.OrderTopic),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		// panic vì đây là setup-time, không nên tiếp tục chạy khi thiếu Kafka
		panic(err)
	}

	return &OrderProducer{
		client: client,
		topic:  cfg.OrderTopic,
	}
}

func (p *OrderProducer) PublishOrder(ctx context.Context, payload []byte) error {
	rec := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(uuid.NewString()),
		Value:     payload,
		Timestamp: time.Now().UTC(),
	}
	// ProduceSync trả về slice lỗi, chỉ dùng 1 record nên lấy phần tử đầu
	results := p.client.ProduceSync(ctx, rec)
	return results.FirstErr()
}

func (p *OrderProducer) Close(ctx context.Context) error {
	p.client.Close()
	return nil
}
