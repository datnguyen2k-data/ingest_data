package kafka

import (
	"context"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"ingest_data/internal/config"
)

type OrderProducer struct {
	writer *kafkago.Writer
}

func NewOrderProducer(cfg config.KafkaConfig) *OrderProducer {
	return &OrderProducer{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(cfg.Brokers...),
			Topic:    cfg.OrderTopic,
			Balancer: &kafkago.LeastBytes{},
		},
	}
}

func (p *OrderProducer) PublishOrder(ctx context.Context, payload []byte) error {
	msg := kafkago.Message{
		Key:   []byte(uuid.NewString()),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	return p.writer.WriteMessages(ctx, msg)
}

func (p *OrderProducer) Close(ctx context.Context) error {
	return p.writer.Close()
}
