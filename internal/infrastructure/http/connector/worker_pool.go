package connector

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkerPool quản lý pool các workers để xử lý concurrent requests
type WorkerPool struct {
	workers      int
	requestQueue chan *WorkRequest
	resultQueue  chan *WorkResult
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// WorkRequest đại diện cho một request cần xử lý
type WorkRequest struct {
	ID      int
	Request *HTTPRequest
	Task    *PollingTask
}

// WorkResult đại diện cho kết quả xử lý request
type WorkResult struct {
	RequestID int
	Records   []Record
	Error     error
	Duration  time.Duration
}

// NewWorkerPool tạo worker pool mới
func NewWorkerPool(ctx context.Context, workers int, queueSize int) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 100
	}

	// Context sẽ được set khi Start
	return &WorkerPool{
		workers:      workers,
		requestQueue: make(chan *WorkRequest, queueSize),
		resultQueue:  make(chan *WorkResult, queueSize),
		ctx:          ctx,
		cancel:       func() {}, // Placeholder, sẽ được set khi Start
	}
}

// Start khởi động worker pool
func (wp *WorkerPool) Start() {
	// Tạo context mới nếu chưa có
	if wp.ctx == nil {
		wp.ctx, wp.cancel = context.WithCancel(context.Background())
	}
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// Stop dừng worker pool
func (wp *WorkerPool) Stop() {
	close(wp.requestQueue)
	wp.cancel()
	wp.wg.Wait()
	close(wp.resultQueue)
}

// Submit gửi request vào queue
func (wp *WorkerPool) Submit(req *WorkRequest) error {
	select {
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	case wp.requestQueue <- req:
		return nil
	}
}

// GetResult lấy kết quả từ result queue
func (wp *WorkerPool) GetResult() <-chan *WorkResult {
	return wp.resultQueue
}

// worker xử lý requests
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case req, ok := <-wp.requestQueue:
			if !ok {
				return
			}

			start := time.Now()
			result := &WorkResult{
				RequestID: req.ID,
			}

			// Thực hiện HTTP request
			resp, err := req.Task.connector.httpClient.Do(wp.ctx, req.Request)
			if err != nil {
				result.Error = fmt.Errorf("http request failed: %w", err)
				result.Duration = time.Since(start)
				wp.sendResult(result)
				continue
			}

			// Kiểm tra status code
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				result.Error = fmt.Errorf("http status %d: %s", resp.StatusCode, string(resp.Body))
				result.Duration = time.Since(start)
				wp.sendResult(result)
				continue
			}

			// Parse response thành records
			records, err := req.Task.responseParser(resp)
			if err != nil {
				result.Error = fmt.Errorf("parse response: %w", err)
				result.Duration = time.Since(start)
				wp.sendResult(result)
				continue
			}

			result.Records = records
			result.Duration = time.Since(start)
			wp.sendResult(result)
		}
	}
}

// sendResult gửi kết quả vào result queue
func (wp *WorkerPool) sendResult(result *WorkResult) {
	select {
	case <-wp.ctx.Done():
	case wp.resultQueue <- result:
	}
}

// BatchProcessor xử lý batch records với goroutine và channel
type BatchProcessor struct {
	batchSize    int
	batchTimeout time.Duration
	recordsChan  chan Record
	batchChan    chan []Record
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	processor    func([]Record) error
}

// NewBatchProcessor tạo batch processor mới
func NewBatchProcessor(ctx context.Context, batchSize int, batchTimeout time.Duration, processor func([]Record) error) *BatchProcessor {
	if batchSize <= 0 {
		batchSize = 100
	}
	if batchTimeout <= 0 {
		batchTimeout = 5 * time.Second
	}

	// Context sẽ được set khi Start
	bp := &BatchProcessor{
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		recordsChan:  make(chan Record, batchSize*2),
		batchChan:    make(chan []Record, 10),
		ctx:          ctx,
		cancel:       func() {}, // Placeholder
		processor:    processor,
	}

	return bp
}

// Start khởi động batch processor
func (bp *BatchProcessor) Start() {
	// Tạo context mới nếu chưa có
	if bp.ctx == nil {
		bp.ctx, bp.cancel = context.WithCancel(context.Background())
	}
	// Start batch collector
	bp.wg.Add(1)
	go bp.collectBatches()

	// Start batch processor workers
	bp.wg.Add(1)
	go bp.processBatches()
}

// Submit gửi record vào batch processor
func (bp *BatchProcessor) Submit(record Record) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	case bp.recordsChan <- record:
		return nil
	}
}

// SubmitBatch gửi nhiều records cùng lúc
func (bp *BatchProcessor) SubmitBatch(records []Record) error {
	for _, record := range records {
		if err := bp.Submit(record); err != nil {
			return err
		}
	}
	return nil
}

// collectBatches thu thập records thành batches
func (bp *BatchProcessor) collectBatches() {
	defer bp.wg.Done()
	defer close(bp.batchChan)

	batch := make([]Record, 0, bp.batchSize)
	ticker := time.NewTicker(bp.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-bp.ctx.Done():
			// Flush batch còn lại
			if len(batch) > 0 {
				bp.sendBatch(batch)
			}
			return

		case record, ok := <-bp.recordsChan:
			if !ok {
				// Channel đã đóng, flush batch còn lại
				if len(batch) > 0 {
					bp.sendBatch(batch)
				}
				return
			}

			batch = append(batch, record)

			// Gửi batch khi đủ size
			if len(batch) >= bp.batchSize {
				bp.sendBatch(batch)
				batch = make([]Record, 0, bp.batchSize)
				ticker.Reset(bp.batchTimeout)
			}

		case <-ticker.C:
			// Flush batch theo timeout
			if len(batch) > 0 {
				bp.sendBatch(batch)
				batch = make([]Record, 0, bp.batchSize)
			}
		}
	}
}

// sendBatch gửi batch vào channel
func (bp *BatchProcessor) sendBatch(batch []Record) {
	select {
	case <-bp.ctx.Done():
	case bp.batchChan <- batch:
	}
}

// processBatches xử lý batches
func (bp *BatchProcessor) processBatches() {
	defer bp.wg.Done()

	for {
		select {
		case <-bp.ctx.Done():
			return
		case batch, ok := <-bp.batchChan:
			if !ok {
				return
			}

			if bp.processor != nil {
				if err := bp.processor(batch); err != nil {
					// Log error hoặc gửi đến error handler
					fmt.Printf("Batch processing error: %v\n", err)
				}
			}
		}
	}
}

// Stop dừng batch processor
func (bp *BatchProcessor) Stop() {
	close(bp.recordsChan)
	bp.cancel()
	bp.wg.Wait()
}

// GetPendingCount trả về số lượng records đang chờ xử lý
func (bp *BatchProcessor) GetPendingCount() int {
	return len(bp.recordsChan)
}

