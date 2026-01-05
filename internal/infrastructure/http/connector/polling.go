package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// HTTPPollingConnector là connector để polling dữ liệu từ HTTP API
// Dựa trên kiến trúc của Confluent HTTP Source Connector
type HTTPPollingConnector struct {
	config         ConnectorConfig
	httpClient     *HTTPClient
	offsetManager  *OffsetManager
	tasks          []*PollingTask
	workerPool     *WorkerPool
	batchProcessor *BatchProcessor
}

// PollingConfig cấu hình cho polling connector
type PollingConfig struct {
	ConnectorConfig
	URL              string
	PollInterval     time.Duration
	RequestBuilder   func(offset Offset) (*HTTPRequest, error) // Hàm để build request dựa trên offset
	ResponseParser   func(*HTTPResponse) ([]Record, error)      // Hàm để parse response thành records
	ErrorHandler     func(error) error                          // Hàm xử lý lỗi
	// Concurrency settings
	ConcurrentPolls  int           // Số lượng poll đồng thời (0 = sequential)
	BatchProcessor   func([]Record) error // Hàm xử lý batch records (optional)
}

// NewHTTPPollingConnector tạo polling connector mới
func NewHTTPPollingConnector(config PollingConfig) (*HTTPPollingConnector, error) {
	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if config.RequestBuilder == nil {
		return nil, fmt.Errorf("RequestBuilder is required")
	}
	if config.ResponseParser == nil {
		return nil, fmt.Errorf("ResponseParser is required")
	}

	// Tạo worker pool nếu có cấu hình concurrent
	var workerPool *WorkerPool
	if config.HTTPConfig.MaxConcurrentRequests > 0 {
		ctx := context.Background() // Sẽ được cập nhật khi Start
		workerPool = NewWorkerPool(ctx, config.HTTPConfig.MaxConcurrentRequests, config.HTTPConfig.RequestQueueSize)
	}

	// Tạo batch processor nếu có
	var batchProcessor *BatchProcessor
	if config.BatchProcessor != nil {
		ctx := context.Background() // Sẽ được cập nhật khi Start
		batchSize := config.HTTPConfig.BatchSize
		if batchSize <= 0 {
			batchSize = 100
		}
		batchTimeout := config.HTTPConfig.BatchTimeout
		if batchTimeout <= 0 {
			batchTimeout = 5 * time.Second
		}
		batchProcessor = NewBatchProcessor(ctx, batchSize, batchTimeout, config.BatchProcessor)
	}

	connector := &HTTPPollingConnector{
		config:         config.ConnectorConfig,
		httpClient:     NewHTTPClient(config.HTTPConfig),
		offsetManager:  NewOffsetManager(config.OffsetConfig),
		tasks:          make([]*PollingTask, 0),
		workerPool:     workerPool,
		batchProcessor: batchProcessor,
	}

	// Tạo tasks
	tasksMax := config.TasksMax
	if tasksMax <= 0 {
		tasksMax = 1
	}

		for i := 0; i < tasksMax; i++ {
		task := &PollingTask{
			id:              i,
			connector:       connector,
			url:             config.URL,
			pollInterval:    config.PollInterval,
			requestBuilder:  config.RequestBuilder,
			responseParser:  config.ResponseParser,
			errorHandler:    config.ErrorHandler,
			topicName:       config.TopicName,
			topicPattern:    config.TopicPattern,
			concurrentPolls: config.ConcurrentPolls,
		}
		connector.tasks = append(connector.tasks, task)
	}

	return connector, nil
}

// ValidateConfig kiểm tra cấu hình
func (c *HTTPPollingConnector) ValidateConfig() error {
	if c.config.TopicName == "" && c.config.TopicPattern == "" {
		return fmt.Errorf("TopicName or TopicPattern is required")
	}
	return nil
}

// GetConnectorClass trả về tên class
func (c *HTTPPollingConnector) GetConnectorClass() string {
	if c.config.ConnectorClass != "" {
		return c.config.ConnectorClass
	}
	return "HTTPPollingConnector"
}

// Start khởi động connector
func (c *HTTPPollingConnector) Start(ctx context.Context) error {
	// Khởi động worker pool nếu có
	if c.workerPool != nil {
		c.workerPool.ctx, c.workerPool.cancel = context.WithCancel(ctx)
		c.workerPool.Start()
	}

	// Khởi động batch processor nếu có
	if c.batchProcessor != nil {
		c.batchProcessor.ctx, c.batchProcessor.cancel = context.WithCancel(ctx)
		c.batchProcessor.Start()
	}

	// Khởi động tasks
	for _, task := range c.tasks {
		go func(t *PollingTask) {
			if err := t.Run(ctx); err != nil {
				// Log error hoặc gửi đến error handler
				fmt.Printf("PollingTask %d error: %v\n", t.id, err)
			}
		}(task)
	}
	return nil
}

// Stop dừng connector
func (c *HTTPPollingConnector) Stop(ctx context.Context) error {
	// Dừng worker pool
	if c.workerPool != nil {
		c.workerPool.Stop()
	}

	// Dừng batch processor
	if c.batchProcessor != nil {
		c.batchProcessor.Stop()
	}

	// Tasks sẽ tự dừng khi context bị cancel
	return nil
}

// GetTasks trả về danh sách tasks
func (c *HTTPPollingConnector) GetTasks() []*PollingTask {
	return c.tasks
}

// PollingTask là task thực hiện polling
type PollingTask struct {
	id              int
	connector       *HTTPPollingConnector
	url             string
	pollInterval    time.Duration
	requestBuilder  func(offset Offset) (*HTTPRequest, error)
	responseParser  func(*HTTPResponse) ([]Record, error)
	errorHandler    func(error) error
	topicName       string
	topicPattern    string
	recordsChan     chan []Record
	errorChan       chan error
	concurrentPolls int // Số lượng poll đồng thời
}

// Run chạy polling task
func (t *PollingTask) Run(ctx context.Context) error {
	if t.pollInterval <= 0 {
		t.pollInterval = 10 * time.Second
	}

	// Nếu có concurrent polls, sử dụng concurrent polling
	if t.concurrentPolls > 0 {
		return t.runConcurrent(ctx)
	}

	// Sequential polling
	return t.runSequential(ctx)
}

// runSequential chạy polling tuần tự
func (t *PollingTask) runSequential(ctx context.Context) error {
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Kiểm tra cursor-based: nếu không còn dữ liệu thì dừng
			offset := t.connector.offsetManager.GetOffset()
			if offset.Mode == OffsetModeCursorBased && !t.connector.offsetManager.HasMoreData() {
				return nil
			}

			records, err := t.Poll(ctx)
			if err != nil {
				if t.errorHandler != nil {
					if handleErr := t.errorHandler(err); handleErr != nil {
						return handleErr
					}
				} else {
					return err
				}
				continue
			}

			// Xử lý records
			if err := t.handleRecords(records); err != nil {
				return err
			}

			// Kiểm tra lại sau khi poll: nếu cursor-based và không còn dữ liệu thì dừng
			offset = t.connector.offsetManager.GetOffset()
			if offset.Mode == OffsetModeCursorBased && !t.connector.offsetManager.HasMoreData() {
				return nil
			}
		}
	}
}

// runConcurrent chạy polling đồng thời
func (t *PollingTask) runConcurrent(ctx context.Context) error {
	// Tạo semaphore để giới hạn số lượng concurrent polls
	sem := make(chan struct{}, t.concurrentPolls)
	resultChan := make(chan pollResult, t.concurrentPolls*2)
	var wg sync.WaitGroup

	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	// Goroutine để xử lý results
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-resultChan:
				if !ok {
					return
				}
				if result.err != nil {
					if t.errorHandler != nil {
						_ = t.errorHandler(result.err)
					}
					continue
				}
				_ = t.handleRecords(result.records)
			}
		}
	}()

	// Main polling loop
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			close(resultChan)
			<-done
			return ctx.Err()

		case <-ticker.C:
			// Kiểm tra cursor-based
			offset := t.connector.offsetManager.GetOffset()
			if offset.Mode == OffsetModeCursorBased && !t.connector.offsetManager.HasMoreData() {
				wg.Wait()
				close(resultChan)
				<-done
				return nil
			}

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()

					records, err := t.Poll(ctx)
					select {
					case resultChan <- pollResult{records: records, err: err}:
					case <-ctx.Done():
					}
				}()
			case <-ctx.Done():
				wg.Wait()
				close(resultChan)
				<-done
				return ctx.Err()
			}
		}
	}
}

// pollResult kết quả của một lần poll
type pollResult struct {
	records []Record
	err     error
}

// handleRecords xử lý records (gửi vào channel hoặc batch processor)
func (t *PollingTask) handleRecords(records []Record) error {
	// Gửi vào batch processor nếu có
	if t.connector.batchProcessor != nil {
		return t.connector.batchProcessor.SubmitBatch(records)
	}

	// Gửi vào channel nếu có
	if t.recordsChan != nil {
		select {
		case t.recordsChan <- records:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending records to channel")
		}
	}

	return nil
}

// Poll thực hiện một lần polling
func (t *PollingTask) Poll(ctx context.Context) ([]Record, error) {
	// Lấy offset hiện tại
	offset := t.connector.offsetManager.GetOffset()

	// Build request dựa trên offset
	req, err := t.requestBuilder(offset)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	// Thực hiện HTTP request (worker pool được sử dụng ở level cao hơn nếu cần)
	resp, err := t.connector.httpClient.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}

	// Kiểm tra status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, string(resp.Body))
	}

	// Parse response thành records
	records, err := t.responseParser(resp)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	// Cập nhật offset (bao gồm cursor nếu là cursor-based)
	if err := t.updateOffset(len(records), resp); err != nil {
		return nil, fmt.Errorf("update offset: %w", err)
	}

	// Set topic cho records
	topic := t.topicName
	if topic == "" {
		topic = t.topicPattern // Có thể cần resolve pattern
	}

	for i := range records {
		if records[i].Topic == "" {
			records[i].Topic = topic
		}
	}

	return records, nil
}

// updateOffset cập nhật offset sau khi poll
func (t *PollingTask) updateOffset(recordCount int, resp *HTTPResponse) error {
	offset := t.connector.offsetManager.GetOffset()
	
	switch offset.Mode {
	case OffsetModeSimpleIncrementing:
		return t.connector.offsetManager.Increment(recordCount)
	case OffsetModeTimestamp:
		return t.connector.offsetManager.UpdateTimestamp(time.Now())
	case OffsetModeCursorBased:
		// Extract cursor từ response
		config := t.connector.offsetManager.config
		if config.CursorExtractor != nil {
			cursor, hasMore, err := config.CursorExtractor(resp)
			if err != nil {
				return fmt.Errorf("extract cursor: %w", err)
			}
			return t.connector.offsetManager.UpdateCursor(cursor, hasMore)
		}
		// Nếu không có extractor, sử dụng default extractor
		cursor, hasMore, err := DefaultCursorExtractor(resp)
		if err != nil {
			return fmt.Errorf("extract cursor: %w", err)
		}
		return t.connector.offsetManager.UpdateCursor(cursor, hasMore)
	default:
		// Không cần cập nhật cho các mode khác
		return nil
	}
}

// GetOffset trả về offset hiện tại
func (t *PollingTask) GetOffset() Offset {
	return t.connector.offsetManager.GetOffset()
}

// SetOffset thiết lập offset
func (t *PollingTask) SetOffset(offset Offset) error {
	return t.connector.offsetManager.SetOffset(offset)
}

// SetRecordsChannel thiết lập channel để gửi records
func (t *PollingTask) SetRecordsChannel(ch chan []Record) {
	t.recordsChan = ch
}

// SetErrorChannel thiết lập channel để gửi errors
func (t *PollingTask) SetErrorChannel(ch chan error) {
	t.errorChan = ch
}

// DefaultCursorExtractor extract cursor từ response mặc định
// Hỗ trợ các format phổ biến:
// - { "next_cursor": "abc123", "has_more": true }
// - { "pagination": { "next_cursor": "abc123", "has_more": true } }
// - { "cursor": "abc123", "has_next": true }
func DefaultCursorExtractor(resp *HTTPResponse) (string, bool, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(resp.Body, &data); err != nil {
		return "", false, fmt.Errorf("unmarshal json: %w", err)
	}

	var cursor string
	var hasMore bool

	// Thử các format phổ biến
	// Format 1: { "next_cursor": "...", "has_more": true }
	if c, ok := data["next_cursor"].(string); ok {
		cursor = c
		if hm, ok := data["has_more"].(bool); ok {
			hasMore = hm
		} else {
			hasMore = cursor != "" // Nếu có cursor thì mặc định là có more
		}
		return cursor, hasMore, nil
	}

	// Format 2: { "pagination": { "next_cursor": "...", "has_more": true } }
	if pagination, ok := data["pagination"].(map[string]interface{}); ok {
		if c, ok := pagination["next_cursor"].(string); ok {
			cursor = c
			if hm, ok := pagination["has_more"].(bool); ok {
				hasMore = hm
			} else {
				hasMore = cursor != ""
			}
			return cursor, hasMore, nil
		}
	}

	// Format 3: { "cursor": "...", "has_next": true }
	if c, ok := data["cursor"].(string); ok {
		cursor = c
		if hn, ok := data["has_next"].(bool); ok {
			hasMore = hn
		} else {
			hasMore = cursor != ""
		}
		return cursor, hasMore, nil
	}

	// Format 4: { "next": "..." } (chỉ có cursor, không có has_more)
	if c, ok := data["next"].(string); ok {
		cursor = c
		hasMore = cursor != ""
		return cursor, hasMore, nil
	}

	// Không tìm thấy cursor, trả về empty và hasMore = false
	return "", false, nil
}

// DefaultJSONResponseParser parse JSON response mặc định
// Giả định response là array hoặc object có field "data" là array
func DefaultJSONResponseParser(resp *HTTPResponse) ([]Record, error) {
	var data interface{}
	if err := json.Unmarshal(resp.Body, &data); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}

	var items []interface{}
	switch v := data.(type) {
	case []interface{}:
		items = v
	case map[string]interface{}:
		// Thử lấy field "data"
		if d, ok := v["data"].([]interface{}); ok {
			items = d
		} else {
			// Nếu không có "data", coi toàn bộ object là một item
			items = []interface{}{v}
		}
	default:
		return nil, fmt.Errorf("unexpected response format")
	}

	records := make([]Record, 0, len(items))
	for _, item := range items {
		valueBytes, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("marshal item: %w", err)
		}

		records = append(records, Record{
			Value:     valueBytes,
			Timestamp: time.Now(),
		})
	}

	return records, nil
}

