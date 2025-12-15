package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"ingest_data/internal/config"

	"github.com/stretchr/testify/require"
)

func TestOrderProducer_Publish(t *testing.T) {
	cfg, err := config.Load()
	require.NoError(t, err, "load config failed")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	producer := NewOrderProducer(cfg.Kafka)
	defer producer.Close(ctx)

	payload := []byte(fmt.Sprintf(`{"health":"ok","ts":%d}`, time.Now().Unix()))

	err = producer.PublishOrder(ctx, payload)
	require.NoError(t, err, "publish to kafka failed")
}
