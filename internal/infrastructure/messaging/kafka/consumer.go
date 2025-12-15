package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	app "ingest_data/internal/application/order"
	"ingest_data/internal/config"
	domain "ingest_data/internal/domain/order"
)

type OrderConsumer struct {
	client  *kgo.Client
	handler *app.Service
}

func NewOrderConsumer(cfg config.KafkaConfig, handler *app.Service) *OrderConsumer {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.OrderTopic),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	return &OrderConsumer{
		client:  client,
		handler: handler,
	}
}

func (c *OrderConsumer) Start(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("kafka fetch error: %v", errs)
		}

		for _, fetch := range fetches {
			for _, topic := range fetch.Topics {
				for _, part := range topic.Partitions {
					for _, rec := range part.Records {
						var order domain.Order
						if err := json.Unmarshal(rec.Value, &order); err != nil {
							return fmt.Errorf("decode message: %w", err)
						}
						if err := c.handler.HandleConsumedOrder(ctx, &order); err != nil {
							return fmt.Errorf("handle order: %w", err)
						}
						// commit offset for this record
						if err := c.client.CommitRecords(ctx, rec); err != nil {
							return fmt.Errorf("commit offset: %w", err)
						}
					}
				}
			}
		}
	}
}

func (c *OrderConsumer) Close() {
	c.client.Close()
}
