package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SourceConnector là interface cơ bản cho tất cả các HTTP connector
// Dựa trên kiến trúc của Confluent Kafka HTTP Source Connector
type SourceConnector interface {
	// ValidateConfig kiểm tra cấu hình có hợp lệ không
	ValidateConfig() error

	// GetConnectorClass trả về tên class của connector
	GetConnectorClass() string

	// Start khởi động connector
	Start(ctx context.Context) error

	// Stop dừng connector
	Stop(ctx context.Context) error
}

// SourceTask là interface cho task thực hiện công việc cụ thể
type SourceTask interface {
	// Poll thực hiện polling dữ liệu từ source
	Poll(ctx context.Context) ([]Record, error)

	// GetOffset lấy offset hiện tại
	GetOffset() Offset

	// SetOffset thiết lập offset
	SetOffset(offset Offset) error
}

// Record đại diện cho một record được produce vào Kafka
type Record struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Headers   map[string]string
	Timestamp time.Time
}

// Offset đại diện cho offset để quản lý vị trí đọc dữ liệu
// Hỗ trợ các mode: SIMPLE_INCREMENTING, TIMESTAMP, CUSTOM, CURSOR_BASED
type Offset struct {
	Mode      OffsetMode
	Value     interface{} // int64 cho incrementing, time.Time cho timestamp, string cho custom/cursor
	LastCount int         // Số lượng records trong lần poll cuối
	HasMore   bool        // Cho cursor-based: có thêm dữ liệu không
}

// OffsetMode định nghĩa các chế độ quản lý offset
type OffsetMode string

const (
	OffsetModeSimpleIncrementing OffsetMode = "SIMPLE_INCREMENTING"
	OffsetModeTimestamp          OffsetMode = "TIMESTAMP"
	OffsetModeCustom             OffsetMode = "CUSTOM"
	OffsetModeCursorBased        OffsetMode = "CURSOR_BASED"
	OffsetModeNone               OffsetMode = "NONE"
)

// HTTPRequest đại diện cho HTTP request
type HTTPRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    io.Reader
	Timeout time.Duration
}

// HTTPResponse đại diện cho HTTP response
type HTTPResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	Request    *HTTPRequest
}

// HTTPClientConfig cấu hình cho HTTP client
type HTTPClientConfig struct {
	Timeout             time.Duration
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	DialTimeout         time.Duration
	KeepAlive           time.Duration
	RetryMaxAttempts    int
	RetryBackoff        time.Duration
}

// ConnectorConfig cấu hình chung cho connector
type ConnectorConfig struct {
	ConnectorClass string
	TopicName      string
	TopicPattern   string
	TasksMax       int
	HTTPConfig     HTTPClientConfig
	OffsetConfig   OffsetConfig
}

// ValidateConfig kiểm tra cấu hình connector
func (c *ConnectorConfig) ValidateConfig() error {
	if c.TopicName == "" && c.TopicPattern == "" {
		return fmt.Errorf("TopicName or TopicPattern is required")
	}
	return nil
}

// OffsetConfig cấu hình cho offset management
type OffsetConfig struct {
	Mode         OffsetMode
	InitialValue interface{}
	IncrementBy  int // Cho SIMPLE_INCREMENTING
	// Cho CURSOR_BASED:
	CursorParamName string                                    // Tên parameter để gửi cursor trong request (mặc định: "cursor")
	CursorExtractor func(*HTTPResponse) (string, bool, error) // Hàm extract cursor từ response (cursor, hasMore, error)
}

// WebhookConfig cấu hình cho webhook connector
type WebhookConfig struct {
	Port            int
	Path            string
	AllowedMethods  []string
	AuthToken       string
	ValidateRequest func(*http.Request) error
	TopicResolver   func(*http.Request) string // Dynamic topic determination
}
