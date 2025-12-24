package connector

import (
	"fmt"
)

// BaseHTTPConnector là struct cơ bản có thể kế thừa cho các connector
// Cung cấp các chức năng chung
type BaseHTTPConnector struct {
	config        ConnectorConfig
	httpClient    *HTTPClient
	offsetManager *OffsetManager
}

// NewBaseHTTPConnector tạo base connector
func NewBaseHTTPConnector(config ConnectorConfig) *BaseHTTPConnector {
	return &BaseHTTPConnector{
		config:        config,
		httpClient:    NewHTTPClient(config.HTTPConfig),
		offsetManager: NewOffsetManager(config.OffsetConfig),
	}
}

// GetConfig trả về cấu hình
func (b *BaseHTTPConnector) GetConfig() ConnectorConfig {
	return b.config
}

// GetHTTPClient trả về HTTP client
func (b *BaseHTTPConnector) GetHTTPClient() *HTTPClient {
	return b.httpClient
}

// GetOffsetManager trả về offset manager
func (b *BaseHTTPConnector) GetOffsetManager() *OffsetManager {
	return b.offsetManager
}

// ValidateConfig kiểm tra cấu hình cơ bản
func (b *BaseHTTPConnector) ValidateConfig() error {
	if b.config.TopicName == "" && b.config.TopicPattern == "" {
		return fmt.Errorf("TopicName or TopicPattern is required")
	}
	return nil
}

