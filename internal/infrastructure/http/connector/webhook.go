package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// WebhookConnector là connector để nhận webhook events
// Dựa trên kiến trúc của Webhook Source Connector
type WebhookConnector struct {
	config     ConnectorConfig
	webhookConfig WebhookConfig
	server     *http.Server
	recordsChan chan []Record
	errorChan   chan error
}

// NewWebhookConnector tạo webhook connector mới
func NewWebhookConnector(config ConnectorConfig, webhookConfig WebhookConfig) (*WebhookConnector, error) {
	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if webhookConfig.Port <= 0 {
		webhookConfig.Port = 8080
	}
	if webhookConfig.Path == "" {
		webhookConfig.Path = "/webhook"
	}
	if len(webhookConfig.AllowedMethods) == 0 {
		webhookConfig.AllowedMethods = []string{http.MethodPost}
	}

	connector := &WebhookConnector{
		config:        config,
		webhookConfig: webhookConfig,
		recordsChan:   make(chan []Record, 100),
		errorChan:     make(chan error, 100),
	}

	mux := http.NewServeMux()
	handler := &WebhookHandler{
		connector: connector,
	}
	mux.HandleFunc(webhookConfig.Path, handler.Handle)

	connector.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", webhookConfig.Port),
		Handler: mux,
	}

	return connector, nil
}

// ValidateConfig kiểm tra cấu hình
func (c *WebhookConnector) ValidateConfig() error {
	if c.config.TopicName == "" && c.config.TopicPattern == "" {
		return fmt.Errorf("TopicName or TopicPattern is required")
	}
	return nil
}

// GetConnectorClass trả về tên class
func (c *WebhookConnector) GetConnectorClass() string {
	if c.config.ConnectorClass != "" {
		return c.config.ConnectorClass
	}
	return "WebhookConnector"
}

// Start khởi động webhook server
func (c *WebhookConnector) Start(ctx context.Context) error {
	go func() {
		if err := c.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			select {
			case c.errorChan <- err:
			default:
			}
		}
	}()

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.server.Shutdown(shutdownCtx)
	}()

	return nil
}

// Stop dừng webhook server
func (c *WebhookConnector) Stop(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.server.Shutdown(shutdownCtx)
}

// GetRecordsChannel trả về channel để nhận records
func (c *WebhookConnector) GetRecordsChannel() <-chan []Record {
	return c.recordsChan
}

// GetErrorChannel trả về channel để nhận errors
func (c *WebhookConnector) GetErrorChannel() <-chan error {
	return c.errorChan
}

// WebhookHandler xử lý HTTP requests từ webhook
type WebhookHandler struct {
	connector *WebhookConnector
}

// Handle xử lý webhook request
func (h *WebhookHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Kiểm tra method
	methodAllowed := false
	for _, m := range h.connector.webhookConfig.AllowedMethods {
		if r.Method == m {
			methodAllowed = true
			break
		}
	}
	if !methodAllowed {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate request nếu có
	if h.connector.webhookConfig.ValidateRequest != nil {
		if err := h.connector.webhookConfig.ValidateRequest(r); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
			return
		}
	}

	// Xác định topic (dynamic topic determination)
	topic := h.connector.config.TopicName
	if h.connector.webhookConfig.TopicResolver != nil {
		if resolvedTopic := h.connector.webhookConfig.TopicResolver(r); resolvedTopic != "" {
			topic = resolvedTopic
		}
	}

	// Đọc request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Read body failed: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse records từ request
	records, err := h.parseRequest(r, bodyBytes, topic)
	if err != nil {
		http.Error(w, fmt.Sprintf("Parse request failed: %v", err), http.StatusBadRequest)
		return
	}

	// Gửi records đến channel
	select {
	case h.connector.recordsChan <- records:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	case <-time.After(5 * time.Second):
		http.Error(w, "Timeout sending records", http.StatusRequestTimeout)
	}
}

// parseRequest parse request body thành records
func (h *WebhookHandler) parseRequest(r *http.Request, bodyBytes []byte, topic string) ([]Record, error) {
	// Thử parse JSON
	var data interface{}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		// Nếu không phải JSON, tạo record từ raw body
		return []Record{
			{
				Value:     bodyBytes,
				Topic:     topic,
				Timestamp: time.Now(),
				Headers:   h.extractHeaders(r),
			},
		}, nil
	}

	// Parse JSON thành records
	var items []interface{}
	switch v := data.(type) {
	case []interface{}:
		items = v
	case map[string]interface{}:
		// Nếu là object, coi như một record
		items = []interface{}{v}
	default:
		return nil, fmt.Errorf("unexpected data format")
	}

	records := make([]Record, 0, len(items))
	for _, item := range items {
		valueBytes, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("marshal item: %w", err)
		}

		records = append(records, Record{
			Value:     valueBytes,
			Topic:     topic,
			Timestamp: time.Now(),
			Headers:   h.extractHeaders(r),
		})
	}

	return records, nil
}

// extractHeaders trích xuất headers từ request
func (h *WebhookHandler) extractHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	return headers
}

