package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	app "ingest_data/internal/application/order"
	"ingest_data/internal/config"
	domain "ingest_data/internal/domain/order"
)

type OrderConsumer struct {
	reader  *kafkago.Reader
	handler *app.Service
}

func NewOrderConsumer(cfg config.KafkaConfig, handler *app.Service) *OrderConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.OrderTopic,
		MinBytes: 1e3,
		MaxBytes: 1e6,
	})

	return &OrderConsumer{
		reader:  reader,
		handler: handler,
	}
}

func (c *OrderConsumer) Start(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var order domain.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			return fmt.Errorf("decode message: %w", err)
		}

		if err := c.handler.HandleConsumedOrder(ctx, &order); err != nil {
			return fmt.Errorf("handle order: %w", err)
		}
	}
}

func (c *OrderConsumer) Close() {
	_ = c.reader.Close()
}
