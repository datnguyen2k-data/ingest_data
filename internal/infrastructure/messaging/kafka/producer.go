package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"
)

type OrderProducer struct {
	client       *kgo.Client
	topic        string
	logger       logger.Logger
	batchSize    int
	batchTimeout time.Duration
	recordsChan  chan *kgo.Record
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.RWMutex
	closed       bool
}

// NewOrderProducer tạo producer mới với async batching mode
func NewOrderProducer(cfg config.KafkaConfig, log logger.Logger) *OrderProducer {
	// Log cấu hình kết nối
	log.Info("Connecting to Kafka brokers (async mode with batching)",
		logger.Any("brokers", cfg.Brokers),
		logger.String("topic", cfg.OrderTopic))

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.OrderTopic),
		// Thêm các options để cải thiện reliability và performance
		kgo.RequiredAcks(kgo.AllISRAcks()), // Đợi tất cả ISR confirm
		kgo.DisableIdempotentWrite(),      // Có thể bật nếu cần idempotent
		// Batching và compression để tối ưu throughput
		kgo.ProducerBatchMaxBytes(10 * 1024 * 1024),                    // 10MB batch size
		kgo.ProducerLinger(10 * time.Millisecond),                     // Wait 10ms để batch
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),         // Compress để giảm network
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("Failed to create Kafka producer client", logger.Error(err))
		panic(fmt.Errorf("create kafka producer: %w", err))
	}

	ctx, cancel := context.WithCancel(context.Background())

	producer := &OrderProducer{
		client:       client,
		topic:        cfg.OrderTopic,
		logger:       log,
		batchSize:    1000,                      // Batch 1000 messages
		batchTimeout: 100 * time.Millisecond,    // Hoặc flush sau 100ms
		recordsChan:  make(chan *kgo.Record, 10000), // Buffer lớn cho high throughput
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start background goroutine để xử lý async produce
	producer.wg.Add(1)
	go producer.produceLoop()

	log.Info("Successfully created Kafka producer client (async mode)",
		logger.String("topic", cfg.OrderTopic),
		logger.Int("batch_size", producer.batchSize),
		logger.String("batch_timeout", producer.batchTimeout.String()),
		logger.Int("channel_buffer", cap(producer.recordsChan)))

	return producer
}

// produceLoop: Background goroutine để produce messages async với batching
func (p *OrderProducer) produceLoop() {
	defer p.wg.Done()

	batch := make([]*kgo.Record, 0, p.batchSize)
	ticker := time.NewTicker(p.batchTimeout)
	defer ticker.Stop()

	errorCount := 0
	successCount := 0
	lastLogTime := time.Now()

	for {
		select {
		case <-p.ctx.Done():
			// Flush batch cuối cùng trước khi exit
			if len(batch) > 0 {
				p.flushBatch(batch)
			}
			p.logger.Info("Producer loop stopped",
				logger.String("topic", p.topic),
				logger.Int("total_success", successCount),
				logger.Int("total_errors", errorCount))
			return

		case rec := <-p.recordsChan:
			if rec == nil {
				// Channel closed
				continue
			}

			batch = append(batch, rec)

			// Flush khi đủ batch size
			if len(batch) >= p.batchSize {
				success, errors := p.flushBatch(batch)
				successCount += success
				errorCount += errors
				batch = batch[:0] // Reset slice nhưng giữ capacity
				p.logBatchStats(successCount, errorCount, &lastLogTime)
			}

		case <-ticker.C:
			// Flush theo timeout
			if len(batch) > 0 {
				success, errors := p.flushBatch(batch)
				successCount += success
				errorCount += errors
				batch = batch[:0]
				p.logBatchStats(successCount, errorCount, &lastLogTime)
			}
		}
	}
}

func (p *OrderProducer) flushBatch(batch []*kgo.Record) (success, errors int) {
	if len(batch) == 0 {
		return 0, 0
	}

	// Produce async - không block
	// franz-go Produce nhận từng record, nhưng sẽ tự động batch
	// Callbacks được gọi async khi Kafka reply, nên chúng ta không đợi kết quả ở đây
	success = len(batch)
	errors = 0

	// Produce tất cả records trong batch với callback để log errors
	for _, rec := range batch {
		p.client.Produce(p.ctx, rec, func(r *kgo.Record, err error) {
			// Callback được gọi serially khi produce hoàn thành (async)
			if err != nil {
				p.logger.Error("Failed to publish message to Kafka",
					logger.String("topic", p.topic),
					logger.Error(err))
			}
		})
	}

	// Log batch queued (records đã được queue, chưa chắc đã produce thành công)
	if len(batch) > 0 {
		p.logger.Debug("Batch queued to Kafka",
			logger.String("topic", p.topic),
			logger.Int("batch_size", len(batch)))
	}

	// Note: errors sẽ được log trong callbacks, nhưng chúng ta không đợi kết quả ở đây
	// để tránh block. Errors thực tế sẽ được track qua logs.
	return success, errors
}

func (p *OrderProducer) logBatchStats(successCount, errorCount int, lastLogTime *time.Time) {
	// Log stats mỗi 10 giây để tránh spam
	if time.Since(*lastLogTime) >= 10*time.Second {
		p.logger.Info("Producer stats",
			logger.String("topic", p.topic),
			logger.Int("total_success", successCount),
			logger.Int("total_errors", errorCount),
			logger.Int("channel_buffer_used", len(p.recordsChan)),
			logger.Int("channel_buffer_capacity", cap(p.recordsChan)))
		*lastLogTime = time.Now()
	}
}

// PublishOrder: Non-blocking, đẩy vào channel
func (p *OrderProducer) PublishOrder(ctx context.Context, payload []byte) error {
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		return fmt.Errorf("producer is closed")
	}

	if len(payload) == 0 {
		return fmt.Errorf("payload is empty")
	}

	rec := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(uuid.NewString()),
		Value:     payload,
		Timestamp: time.Now().UTC(),
	}

	// Non-blocking send với timeout để tránh backpressure quá lâu
	select {
	case p.recordsChan <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
		// Backpressure: nếu channel đầy quá 1s → báo lỗi
		return fmt.Errorf("kafka producer channel full (backpressure), buffer: %d/%d",
			len(p.recordsChan), cap(p.recordsChan))
	}
}

func (p *OrderProducer) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	p.logger.Info("Closing Kafka producer", logger.String("topic", p.topic))

	// Cancel context để dừng produceLoop
	p.cancel()

	// Đợi produceLoop hoàn thành với timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.client.Close()
		p.logger.Info("Kafka producer closed successfully", logger.String("topic", p.topic))
		return nil
	case <-ctx.Done():
		p.logger.Warn("Context cancelled while closing producer", logger.String("topic", p.topic))
		p.client.Close()
		return ctx.Err()
	case <-time.After(30 * time.Second):
		p.logger.Warn("Timeout waiting for producer to close", logger.String("topic", p.topic))
		p.client.Close()
		return fmt.Errorf("timeout closing producer")
	}
}
