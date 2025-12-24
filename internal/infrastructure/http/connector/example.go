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
				Timeout:            30 * time.Second,
				MaxIdleConns:       100,
				MaxIdleConnsPerHost: 10,
				RetryMaxAttempts:   3,
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
				fmt.Printf("Received record: %s\n", string(record.Value))
				// Produce vào Kafka hoặc xử lý khác
			}
		}
	}()

	// Chạy trong 1 phút
	time.Sleep(1 * time.Minute)
	
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
				fmt.Printf("Received webhook record: %s\n", string(record.Value))
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

