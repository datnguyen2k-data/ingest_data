package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// HTTPPollingConnector là connector để polling dữ liệu từ HTTP API
// Dựa trên kiến trúc của Confluent HTTP Source Connector
type HTTPPollingConnector struct {
	config        ConnectorConfig
	httpClient    *HTTPClient
	offsetManager *OffsetManager
	tasks         []*PollingTask
}

// PollingConfig cấu hình cho polling connector
type PollingConfig struct {
	ConnectorConfig
	URL              string
	PollInterval     time.Duration
	RequestBuilder   func(offset Offset) (*HTTPRequest, error) // Hàm để build request dựa trên offset
	ResponseParser   func(*HTTPResponse) ([]Record, error)      // Hàm để parse response thành records
	ErrorHandler     func(error) error                          // Hàm xử lý lỗi
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

	connector := &HTTPPollingConnector{
		config:        config.ConnectorConfig,
		httpClient:    NewHTTPClient(config.HTTPConfig),
		offsetManager: NewOffsetManager(config.OffsetConfig),
		tasks:         make([]*PollingTask, 0),
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
	// Tasks sẽ tự dừng khi context bị cancel
	return nil
}

// GetTasks trả về danh sách tasks
func (c *HTTPPollingConnector) GetTasks() []*PollingTask {
	return c.tasks
}

// PollingTask là task thực hiện polling
type PollingTask struct {
	id             int
	connector      *HTTPPollingConnector
	url            string
	pollInterval   time.Duration
	requestBuilder func(offset Offset) (*HTTPRequest, error)
	responseParser func(*HTTPResponse) ([]Record, error)
	errorHandler   func(error) error
	topicName      string
	topicPattern   string
	recordsChan    chan []Record
	errorChan      chan error
}

// Run chạy polling task
func (t *PollingTask) Run(ctx context.Context) error {
	if t.pollInterval <= 0 {
		t.pollInterval = 10 * time.Second
	}

	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
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

			// Gửi records đến channel nếu có
			if t.recordsChan != nil {
				select {
				case t.recordsChan <- records:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
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

	// Thực hiện HTTP request
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

	// Cập nhật offset
	if err := t.updateOffset(len(records)); err != nil {
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
func (t *PollingTask) updateOffset(recordCount int) error {
	offset := t.connector.offsetManager.GetOffset()
	
	switch offset.Mode {
	case OffsetModeSimpleIncrementing:
		return t.connector.offsetManager.Increment(recordCount)
	case OffsetModeTimestamp:
		return t.connector.offsetManager.UpdateTimestamp(time.Now())
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

