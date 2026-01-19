package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"
)

type OrderProducer struct {
	producer    *kafka.Producer
	serializer  serde.Serializer
	topic       string
	logger      logger.Logger
	deliveryCha chan kafka.Event
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewOrderProducer creates a new producer using Confluent Kafka Client with Schema Registry
func NewOrderProducer(cfg config.KafkaConfig, log logger.Logger) *OrderProducer {
	log.Info("Connecting to Kafka brokers (confluent-kafka-go)",
		logger.Any("brokers", cfg.Brokers),
		logger.String("schema_registry", cfg.SchemaRegistryURL),
		logger.String("topic", cfg.OrderTopic))

	// 1. Create Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		"client.id":         "pancake-ingest-producer",
		"acks":              "all",
		"compression.type":  "snappy",
		"linger.ms":         10,
	})
	if err != nil {
		log.Fatal("Failed to create Kafka producer", logger.Error(err))
	}

	// 2. Create Schema Registry Client
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistryURL))
	if err != nil {
		log.Fatal("Failed to create Schema Registry client", logger.Error(err))
	}

	// 3. Create Avro Serializer
	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("Failed to create Avro serializer", logger.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())

	op := &OrderProducer{
		producer:    p,
		serializer:  serializer,
		topic:       cfg.OrderTopic,
		logger:      log,
		deliveryCha: make(chan kafka.Event, 10000), // Buffer for delivery reports
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start delivery report handler
	op.wg.Add(1)
	go op.handleDeliveryReports()

	return op
}

func (p *OrderProducer) handleDeliveryReports() {
	defer p.wg.Done()
	logCount := 0

	for {
		select {
		case <-p.ctx.Done():
			return
		case e := <-p.producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					p.logger.Error("Delivery failed",
						logger.String("topic", *ev.TopicPartition.Topic),
						logger.Error(ev.TopicPartition.Error))
				} else {
					// Success
					// Optional: Log periodically to avoid spam
					logCount++
					if logCount%1000 == 0 {
						p.logger.Info("Messages delivered", logger.Int("count", logCount))
					}
				}
			case kafka.Error:
				p.logger.Error("Kafka error", logger.Error(ev))
			}
		}
	}
}

// PublishOrder publishes a generic map directly to Kafka using Avro serializer
// data must be map[string]interface{} compatible with the Avro schema
func (p *OrderProducer) PublishOrder(ctx context.Context, data interface{}) error {
	// Serialize payload
	payload, err := p.serializer.Serialize(p.topic, data)
	if err != nil {
		return fmt.Errorf("serialize avro: %w", err)
	}

	// Produce
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          payload,
		// Key: optional, if we have a key
	}, nil)

	if err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	// Flush happens automatically in background by librdkafka
	// We can manually flush if needed, but for high throughput we rely on "linger.ms"
	return nil
}

func (p *OrderProducer) Close(ctx context.Context) error {
	p.logger.Info("Closing Kafka producer")

	// Wait for pending messages
	p.producer.Flush(5000) // 5s timeout

	p.cancel()
	p.producer.Close()
	p.serializer.Close()

	p.wg.Wait()
	return nil
}

// Helper to register schema manually if needed, but NewGenericSerializer handles it automatically
// provided we pass the schema string. But GenericSerializer usually expects
// the schema to be provided/inferred or we use a SpecificSerializer.
// Actually, NewGenericSerializer allows serializing WITHOUT pre-registering if we provide the schema.
// BUT, the `avro.GenericSerializer` in `confluent-kafka-go` usually works with `rifer.Avro` or `interface{}`.
// Let's ensure we register the schema first to be safe, OR we configure the serializer to auto-register.
// The default behavior of `NewGenericSerializer` is to auto-register schema if using `Serialize` with a record that has schema info?
// Wait, `confluent-kafka-go` GenericSerializer is for *generic*, i.e., `interface{}`.
// How does it know the schema?
// We need to pass the schema string to `Serialize`? No, `Serialize` signature is `Serialize(topic string, value interface{})`.
// The standard Avro serializer infers schema from the object if it's a specific type (generated code).
// For generic maps, we might need `avro.NewGenericSerializer` AND register the schema explicitly, or stick to `goavro` for serialization and just use `confluent-kafka-go` for production?
// NO, the user wants "schema registry aware". That means the payload must have the Magic Byte + Schema ID.
// `confluent-kafka-go` serializer DOES this.
// But we need to tell it WHAT schema to use for the map.
// The `GenericSerializer` might be tricky with just a `map`.
// It's often easier to generic `NewSpecificSerializer` if we had generated code.
// Since we don't have generated code, we need to register the schema manually, get the ID, and then serialize?
// Or: Use `Execute` style?
//
// LET'S CHECK: `confluent-kafka-go/v2/schemaregistry/serde/avro`
// `NewGenericSerializer` takes a `Client`.
// When `Serialize` is called, it usually expects the value to be compatible depending on config.
// If we pass a Map, how does it know the schema?
// Answer: For generic maps, `confluent-kafka-go`'s Avro serializer might NOT support auto-inference.
// WE NEED TO REGISTER SCHEMA MANUALLY and use that to Create the Serializer?
// OR: We can just use `NewSerializer(..., avro.NewSerializerConfig())` if we assume the subject exists?
//
// Actually, for Generic Avro (Map), we often explicitly use `goavro` to get the binary, and then manually prepend the ID?
// NO, that defeats the purpose of "confluent library".
//
// Solution: We will use a workaround or simpler approach.
// We will register the schema manually on startup to get the `SchemaID`.
// Then use `serializer.Serialize`? NO.
//
// UPDATE: `confluent-kafka-go` v2 supports `avro.NewGenericSerializer`.
// But checking docs/examples: It works well with `native` types if we give it the schema?
// Actually, `confluent-kafka-go` doesn't make it easy to pass "Schema String" at `Serialize` time for a Map.
//
// CORRECT APPROACH for Generic Map:
// 1. We must register the schema to get the ID (or subject).
// 2. Actually, we can use the low-level `schemaregistry` client to Register.
// 3. Then we might need to manually construct the envelope if `avro` package fails us?
//
// WAIT, looking at `confluent-kafka-go` examples:
// One can use `serde.Serializer` interface.
// If we use struct tags, it works. We don't have struct tags.
//
// ALTERNATIVE: Use `avro.NewSpecificSerializer` but we need a struct.
// Refactor: Can we define a Go struct that matches the schema?
// Yes, `internal/infrastructure/encoding/avro/schema.go` defines it.
// I can define a `PancakeOrder` struct in Go that matches the Avro tags!
// This is the clean way.
//
// Let's create `internal/domain/pancake_order.go` or put it in `infrastructure`.
// I will define the Struct with Avro tags.
// Then I can use `NewSpecificSerializer`.
// This is much more robust than the Map approach.
