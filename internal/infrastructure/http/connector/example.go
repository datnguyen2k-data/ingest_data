package connector

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// ExampleHTTPPollingConnector minh họa cách sử dụng HTTPPollingConnector
func ExampleHTTPPollingConnector() {
	// Cấu hình connector
	config := PollingConfig{
		ConnectorConfig: ConnectorConfig{
			ConnectorClass: "ExampleHTTPPollingConnector",
			TopicName:      "orders",
			TasksMax:       1,
			HTTPConfig: HTTPClientConfig{
				Timeout:             30 * time.Second,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				RetryMaxAttempts:    3,
			},
			OffsetConfig: OffsetConfig{
				Mode:         OffsetModeSimpleIncrementing,
				InitialValue: int64(0),
				IncrementBy:  1,
			},
		},
		URL:          "https://api.example.com/orders",
		PollInterval: 10 * time.Second,
		RequestBuilder: func(offset Offset) (*HTTPRequest, error) {
			// Build request dựa trên offset
			offsetValue := offset.Value.(int64)
			url := fmt.Sprintf("https://api.example.com/orders?offset=%d&limit=100", offsetValue)

			return &HTTPRequest{
				Method: http.MethodGet,
				URL:    url,
				Headers: map[string]string{
					"Authorization": "Bearer token",
					"Content-Type":  "application/json",
				},
			}, nil
		},
		ResponseParser: DefaultJSONResponseParser,
		ErrorHandler: func(err error) error {
			// Xử lý lỗi, có thể log hoặc retry
			fmt.Printf("Error occurred: %v\n", err)
			return nil // Return nil để tiếp tục polling
		},
	}

	// Tạo connector
	connector, err := NewHTTPPollingConnector(config)
	if err != nil {
		panic(err)
	}

	// Tạo channel để nhận records
	recordsChan := make(chan []Record, 100)

	// Set channel cho task
	tasks := connector.GetTasks()
	if len(tasks) > 0 {
		tasks[0].SetRecordsChannel(recordsChan)
	}

	// Khởi động connector
	ctx := context.Background()
	if err := connector.Start(ctx); err != nil {
		panic(err)
	}

	// Xử lý records
	go func() {
		for records := range recordsChan {
			for _, record := range records {
				fmt.Printf("Received record: %v\n", record.Value)
				// Produce vào Kafka hoặc xử lý khác
			}
		}
	}()

	// Chạy trong 1 phút
	time.Sleep(1 * time.Minute)

	// Dừng connector
	_ = connector.Stop(ctx)
}

// ExampleHTTPPollingConnectorWithCursor minh họa cách sử dụng HTTPPollingConnector với cursor-based pagination
func ExampleHTTPPollingConnectorWithCursor() {
	// Cấu hình connector với cursor-based offset
	config := PollingConfig{
		ConnectorConfig: ConnectorConfig{
			ConnectorClass: "ExampleHTTPPollingConnectorWithCursor",
			TopicName:      "orders",
			TasksMax:       1,
			HTTPConfig: HTTPClientConfig{
				Timeout:             30 * time.Second,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				RetryMaxAttempts:    3,
			},
			OffsetConfig: OffsetConfig{
				Mode:            OffsetModeCursorBased,
				InitialValue:    "",       // Cursor rỗng cho lần đầu
				CursorParamName: "cursor", // Tên parameter trong request
				CursorExtractor: func(resp *HTTPResponse) (string, bool, error) {
					// Custom extractor nếu API có format đặc biệt
					// Ở đây sử dụng default extractor
					return DefaultCursorExtractor(resp)
				},
			},
		},
		URL:          "https://api.example.com/orders",
		PollInterval: 5 * time.Second,
		RequestBuilder: func(offset Offset) (*HTTPRequest, error) {
			// Build request với cursor
			cursor, _, err := func() (string, bool, error) {
				if offset.Value == nil {
					return "", true, nil
				}
				c, ok := offset.Value.(string)
				if !ok {
					return "", false, fmt.Errorf("invalid cursor type")
				}
				return c, offset.HasMore, nil
			}()
			if err != nil {
				return nil, err
			}

			// Build URL với cursor parameter
			url := "https://api.example.com/orders"
			if cursor != "" {
				url = fmt.Sprintf("%s?cursor=%s", url, cursor)
			}

			return &HTTPRequest{
				Method: http.MethodGet,
				URL:    url,
				Headers: map[string]string{
					"Authorization": "Bearer token",
					"Content-Type":  "application/json",
				},
			}, nil
		},
		ResponseParser: func(resp *HTTPResponse) ([]Record, error) {
			// Parse response, có thể có format: { "data": [...], "next_cursor": "...", "has_more": true }
			return DefaultJSONResponseParser(resp)
		},
		ErrorHandler: func(err error) error {
			fmt.Printf("Error occurred: %v\n", err)
			return nil // Return nil để tiếp tục polling
		},
	}

	// Tạo connector
	connector, err := NewHTTPPollingConnector(config)
	if err != nil {
		panic(err)
	}

	// Tạo channel để nhận records
	recordsChan := make(chan []Record, 100)

	// Set channel cho task
	tasks := connector.GetTasks()
	if len(tasks) > 0 {
		tasks[0].SetRecordsChannel(recordsChan)
	}

	// Khởi động connector
	ctx := context.Background()
	if err := connector.Start(ctx); err != nil {
		panic(err)
	}

	// Xử lý records
	go func() {
		for records := range recordsChan {
			for _, record := range records {
				fmt.Printf("Received record: %v\n", record.Value)
				// Produce vào Kafka hoặc xử lý khác
			}
		}
	}()

	// Chạy cho đến khi không còn dữ liệu (hasMore = false) hoặc context bị cancel
	<-ctx.Done()

	// Dừng connector
	_ = connector.Stop(ctx)
}

// ExampleHTTPPollingConnectorWithConcurrency minh họa cách sử dụng HTTPPollingConnector với concurrent polling và batch processing
func ExampleHTTPPollingConnectorWithConcurrency() {
	// Cấu hình connector với concurrent polling và batch processing
	config := PollingConfig{
		ConnectorConfig: ConnectorConfig{
			ConnectorClass: "ExampleHTTPPollingConnectorWithConcurrency",
			TopicName:      "orders",
			TasksMax:       1,
			HTTPConfig: HTTPClientConfig{
				Timeout:               30 * time.Second,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   10,
				RetryMaxAttempts:      3,
				MaxConcurrentRequests: 10,              // Cho phép 10 requests đồng thời
				RequestQueueSize:      100,             // Queue size cho requests
				BatchSize:             50,              // Batch 50 records
				BatchTimeout:          2 * time.Second, // Flush batch sau 2 giây
			},
			OffsetConfig: OffsetConfig{
				Mode:         OffsetModeSimpleIncrementing,
				InitialValue: int64(0),
				IncrementBy:  1,
			},
		},
		URL:             "https://api.example.com/orders",
		PollInterval:    5 * time.Second,
		ConcurrentPolls: 5, // 5 polls đồng thời
		RequestBuilder: func(offset Offset) (*HTTPRequest, error) {
			offsetValue := offset.Value.(int64)
			url := fmt.Sprintf("https://api.example.com/orders?offset=%d&limit=100", offsetValue)

			return &HTTPRequest{
				Method: http.MethodGet,
				URL:    url,
				Headers: map[string]string{
					"Authorization": "Bearer token",
					"Content-Type":  "application/json",
				},
			}, nil
		},
		ResponseParser: DefaultJSONResponseParser,
		ErrorHandler: func(err error) error {
			fmt.Printf("Error occurred: %v\n", err)
			return nil
		},
		// Batch processor để xử lý records theo batch
		BatchProcessor: func(records []Record) error {
			fmt.Printf("Processing batch of %d records\n", len(records))
			// Produce vào Kafka hoặc xử lý batch
			for _, record := range records {
				_ = record // Xử lý record
			}
			return nil
		},
	}

	// Tạo connector
	connector, err := NewHTTPPollingConnector(config)
	if err != nil {
		panic(err)
	}

	// Khởi động connector
	ctx := context.Background()
	if err := connector.Start(ctx); err != nil {
		panic(err)
	}

	// Chạy trong 5 phút
	time.Sleep(5 * time.Minute)

	// Dừng connector
	_ = connector.Stop(ctx)
}

// ExampleWebhookConnector minh họa cách sử dụng WebhookConnector
func ExampleWebhookConnector() {
	// Cấu hình connector
	config := ConnectorConfig{
		ConnectorClass: "ExampleWebhookConnector",
		TopicName:      "webhook_events",
		HTTPConfig: HTTPClientConfig{
			Timeout: 30 * time.Second,
		},
	}

	webhookConfig := WebhookConfig{
		Port:           8080,
		Path:           "/webhook",
		AllowedMethods: []string{http.MethodPost},
		AuthToken:      "secret-token",
		ValidateRequest: func(r *http.Request) error {
			// Validate auth token
			token := r.Header.Get("Authorization")
			if token != "Bearer secret-token" {
				return fmt.Errorf("unauthorized")
			}
			return nil
		},
		TopicResolver: func(r *http.Request) string {
			// Dynamic topic determination dựa trên header
			if topic := r.Header.Get("X-Topic"); topic != "" {
				return topic
			}
			return "webhook_events" // Default topic
		},
	}

	// Tạo connector
	connector, err := NewWebhookConnector(config, webhookConfig)
	if err != nil {
		panic(err)
	}

	// Khởi động connector
	ctx := context.Background()
	if err := connector.Start(ctx); err != nil {
		panic(err)
	}

	// Xử lý records từ webhook
	go func() {
		for records := range connector.GetRecordsChannel() {
			for _, record := range records {
				fmt.Printf("Received webhook record: %v\n", record.Value)
				// Produce vào Kafka hoặc xử lý khác
			}
		}
	}()

	// Xử lý errors
	go func() {
		for err := range connector.GetErrorChannel() {
			fmt.Printf("Webhook error: %v\n", err)
		}
	}()

	// Chạy cho đến khi context bị cancel
	<-ctx.Done()

	// Dừng connector
	_ = connector.Stop(ctx)
}

// ExampleCustomConnector minh họa cách kế thừa BaseHTTPConnector
type CustomConnector struct {
	*BaseHTTPConnector
	customField string
}

// NewCustomConnector tạo custom connector mới
func NewCustomConnector(config ConnectorConfig, customField string) *CustomConnector {
	return &CustomConnector{
		BaseHTTPConnector: NewBaseHTTPConnector(config),
		customField:       customField,
	}
}

// GetConnectorClass implement SourceConnector interface
func (c *CustomConnector) GetConnectorClass() string {
	return "CustomConnector"
}

// Start implement SourceConnector interface
func (c *CustomConnector) Start(ctx context.Context) error {
	// Custom logic để start
	fmt.Printf("Starting custom connector with field: %s\n", c.customField)
	return nil
}

// Stop implement SourceConnector interface
func (c *CustomConnector) Stop(ctx context.Context) error {
	// Custom logic để stop
	fmt.Printf("Stopping custom connector\n")
	return nil
}
