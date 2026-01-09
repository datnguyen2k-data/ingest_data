package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ingest_data/internal/config"
	connector "ingest_data/internal/infrastructure/http/connector"
	kafkaInfra "ingest_data/internal/infrastructure/messaging/kafka"
	"ingest_data/pkg/logger"
)

// Chương trình để polling liên tục orders từ Pancake và đẩy vào Kafka topic pancake_chando_sale_order.
// Sử dụng connector module để polling định kỳ theo interval.
func main() {
	cfg, err := config.Load()
	if err != nil {
		panic("load config failed: " + err.Error())
	}

	// Khởi tạo logger với DI
	log, err := logger.NewZapLogger(cfg.App.Env)
	if err != nil {
		panic("initialize logger failed: " + err.Error())
	}
	defer log.Sync()

	// Validate config
	if cfg.Kafka.OrderTopic == "" {
		log.Fatal("KAFKA_ORDER_TOPIC is empty (ví dụ: pancake_chando_sale_order)")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS is empty (ví dụ: localhost:19092,localhost:29092,localhost:39092)")
	}
	if cfg.Pancake.APIKey == "" || cfg.Pancake.ShopID == "" {
		log.Fatal("PANCAKE_CHANDO_API_KEY or PANCAKE_CHANDO_SHOP_ID is empty")
	}

	log.Info("Starting Pancake ingestion service with connector",
		logger.String("app_name", cfg.App.Name),
		logger.String("env", cfg.App.Env),
		logger.Any("kafka_brokers", cfg.Kafka.Brokers),
		logger.String("kafka_topic", cfg.Kafka.OrderTopic),
		logger.String("pancake_shop_id", cfg.Pancake.ShopID))

	// Setup context với signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Info("Received shutdown signal, stopping...")
		cancel()
	}()

	// Tạo Kafka producer
	producer := kafkaInfra.NewOrderProducer(cfg.Kafka, log)
	defer func() {
		if err := producer.Close(ctx); err != nil {
			log.Error("Failed to close Kafka producer", logger.Error(err))
		}
	}()

	// Tính poll interval từ config
	pollInterval := time.Duration(cfg.Pancake.SleepMS) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 1 * time.Hour // Default 1 hour
	}

	// Time window: lấy orders trong khoảng thời gian này (ví dụ: 7 ngày gần nhất)
	timeWindow := 7 * 24 * time.Hour // 7 ngày

	// Parse base URL
	baseURL, err := url.Parse(cfg.Pancake.BaseURL)
	if err != nil {
		log.Fatal("Invalid PANCAKE_CHANDO_BASE_URL", logger.Error(err))
	}

	log.Info("Connector configuration",
		logger.String("poll_interval", pollInterval.String()),
		logger.String("time_window", timeWindow.String()))

	// Tạo connector config
	connectorConfig := connector.PollingConfig{
		ConnectorConfig: connector.ConnectorConfig{
			ConnectorClass: "PancakeHTTPPollingConnector",
			TopicName:      cfg.Kafka.OrderTopic,
			TasksMax:       1,
			HTTPConfig: connector.HTTPClientConfig{
				Timeout:             30 * time.Second,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				RetryMaxAttempts:    3,
				RetryBackoff:        1 * time.Second,
			},
			OffsetConfig: connector.OffsetConfig{
				Mode:         connector.OffsetModeTimestamp, // Dùng timestamp để track thời gian
				InitialValue: time.Now().UTC().Add(-timeWindow),
			},
		},
		URL:          baseURL.String(),
		PollInterval: pollInterval,

		// RequestBuilder: Xây dựng request cho Pancake API - chỉ lấy page đầu tiên
		// ResponseParser sẽ tự động fetch tất cả các pages còn lại
		RequestBuilder: func(offset connector.Offset) (*connector.HTTPRequest, error) {
			// Lấy startTime từ offset (lần poll trước), endTime là bây giờ
			var startTime time.Time
			if offset.Value != nil {
				if t, ok := offset.Value.(time.Time); ok && !t.IsZero() {
					startTime = t.UTC()
				} else {
					startTime = time.Now().UTC().Add(-timeWindow)
				}
			} else {
				startTime = time.Now().UTC().Add(-timeWindow)
			}
			endTime := time.Now().UTC()

			// Convert sang Unix timestamp (số nguyên) - đúng format như URL mẫu
			startUnix := startTime.Unix()
			endUnix := endTime.Unix()

			// Build URL đúng format: https://pos.pages.fm/api/v1/shops/{shop_id}/orders?...
			u := *baseURL
			u.Path = fmt.Sprintf("%s/shops/%s/orders", baseURL.Path, cfg.Pancake.ShopID)

			// Build query parameters theo đúng format URL mẫu
			q := u.Query()
			q.Set("api_key", cfg.Pancake.APIKey)
			q.Set("startDateTime", fmt.Sprintf("%d", startUnix)) // Unix timestamp (số nguyên)
			q.Set("endDateTime", fmt.Sprintf("%d", endUnix))     // Unix timestamp (số nguyên)
			q.Set("updateStatus", "inserted_at")                 // inserted_at như trong URL mẫu
			q.Set("page_size", fmt.Sprintf("%d", cfg.Pancake.PageSize))
			q.Set("page_number", "1")               // Bắt đầu từ page 1
			q.Set("option_sort", "updated_at_desc") // Sort theo updated_at desc
			u.RawQuery = q.Encode()

			// Log URL (ẩn API key)
			logURL := u.String()
			if cfg.Pancake.APIKey != "" {
				logURL = strings.ReplaceAll(logURL, cfg.Pancake.APIKey, "***")
			}
			log.Info("Building Pancake API request",
				logger.String("url", logURL),
				logger.Int64("start_unix", startUnix),
				logger.Int64("end_unix", endUnix),
				logger.String("start_time", startTime.Format(time.RFC3339)),
				logger.String("end_time", endTime.Format(time.RFC3339)))

			return &connector.HTTPRequest{
				Method: http.MethodGet,
				URL:    u.String(),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}, nil
		},

		// ResponseParser: Parse response và fetch tất cả các pages còn lại
		ResponseParser: func(resp *connector.HTTPResponse) ([]connector.Record, error) {
			// Kiểm tra status code
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				errorMsg := fmt.Sprintf("pancake api status %d: %s", resp.StatusCode, string(resp.Body))
				log.Error("Pancake API error response",
					logger.Int("status_code", resp.StatusCode),
					logger.String("response_body", string(resp.Body)))
				return nil, fmt.Errorf("pancake api error: %s", errorMsg)
			}

			// Parse JSON response
			var body struct {
				Data       []json.RawMessage `json:"data"`
				TotalPages int               `json:"total_pages"`
				PageNumber int               `json:"page_number"`
			}
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				log.Error("Failed to decode Pancake API response",
					logger.String("response_body", string(resp.Body)),
					logger.Error(err))
				return nil, fmt.Errorf("decode response: %w", err)
			}

			log.Info("Pancake API response received",
				logger.Int("page", body.PageNumber),
				logger.Int("orders_count", len(body.Data)),
				logger.Int("total_pages", body.TotalPages))

			allRecords := make([]connector.Record, 0)

			// Thêm records từ page đầu tiên
			for _, item := range body.Data {
				if !json.Valid(item) {
					log.Warn("Invalid JSON in order data, skipping")
					continue
				}
				allRecords = append(allRecords, connector.Record{
					Value:     item,
					Topic:     cfg.Kafka.OrderTopic,
					Timestamp: time.Now().UTC(),
				})
			}

			// Fetch các pages còn lại nếu có
			if body.TotalPages > 1 {
				// Parse URL từ request để lấy các tham số
				reqURL, err := url.Parse(resp.Request.URL)
				if err != nil {
					log.Error("Failed to parse request URL", logger.Error(err))
					return allRecords, nil // Trả về records đã có
				}

				// Fetch các pages còn lại
				httpClient := &http.Client{
					Timeout: 30 * time.Second,
				}

				for page := 2; page <= body.TotalPages; page++ {
					q := reqURL.Query()
					q.Set("page_number", fmt.Sprintf("%d", page))
					reqURL.RawQuery = q.Encode()

					log.Debug("Fetching additional page",
						logger.Int("page", page),
						logger.Int("total_pages", body.TotalPages))

					req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
					if err != nil {
						log.Error("Failed to build request for page",
							logger.Int("page", page),
							logger.Error(err))
						continue
					}

					pageResp, err := httpClient.Do(req)
					if err != nil {
						log.Error("Failed to fetch page",
							logger.Int("page", page),
							logger.Error(err))
						continue
					}

					// Đọc response
					pageBodyBytes, err := io.ReadAll(pageResp.Body)
					pageResp.Body.Close()
					if err != nil {
						log.Error("Failed to read page response",
							logger.Int("page", page),
							logger.Error(err))
						continue
					}

					if pageResp.StatusCode < 200 || pageResp.StatusCode >= 300 {
						log.Error("Pancake API error for page",
							logger.Int("page", page),
							logger.Int("status_code", pageResp.StatusCode),
							logger.String("response_body", string(pageBodyBytes)))
						continue
					}

					var pageBody struct {
						Data []json.RawMessage `json:"data"`
					}
					if err := json.Unmarshal(pageBodyBytes, &pageBody); err != nil {
						log.Error("Failed to decode page response",
							logger.Int("page", page),
							logger.Error(err))
						continue
					}

					log.Info("Pancake API page received",
						logger.Int("page", page),
						logger.Int("orders_count", len(pageBody.Data)))

					// Thêm records từ page này
					for _, item := range pageBody.Data {
						if !json.Valid(item) {
							continue
						}
						allRecords = append(allRecords, connector.Record{
							Value:     item,
							Topic:     cfg.Kafka.OrderTopic,
							Timestamp: time.Now().UTC(),
						})
					}

					// Sleep giữa các pages (như code cũ)
					select {
					case <-ctx.Done():
						log.Warn("Context cancelled while fetching pages")
						return allRecords, ctx.Err()
					case <-time.After(time.Duration(cfg.Pancake.SleepMS) * time.Millisecond):
					}
				}
			}

			log.Info("Completed fetching all pages",
				logger.Int("total_orders", len(allRecords)),
				logger.Int("total_pages", body.TotalPages))

			return allRecords, nil
		},

		// ErrorHandler: Xử lý lỗi
		ErrorHandler: func(err error) error {
			log.Error("Pancake connector error", logger.Error(err))
			// Return nil để tiếp tục polling, không dừng connector
			return nil
		},
	}

	// Tạo connector
	conn, err := connector.NewHTTPPollingConnector(connectorConfig)
	if err != nil {
		log.Fatal("Failed to create connector", logger.Error(err))
	}

	// Tạo channel để nhận records
	recordsChan := make(chan []connector.Record, 100)
	tasks := conn.GetTasks()
	if len(tasks) > 0 {
		tasks[0].SetRecordsChannel(recordsChan)
	}

	// Xử lý records và publish vào Kafka
	go func() {
		totalCount := 0
		for records := range recordsChan {
			batchCount := 0
			for _, record := range records {
				if err := producer.PublishOrder(ctx, record.Value); err != nil {
					log.Error("Failed to publish to Kafka",
						logger.Error(err),
						logger.String("topic", record.Topic),
						logger.Int("payload_size", len(record.Value)))
					continue
				}
				batchCount++
				totalCount++
			}
			if batchCount > 0 {
				log.Info("Published batch to Kafka",
					logger.Int("batch_size", batchCount),
					logger.Int("total_synced", totalCount),
					logger.String("topic", cfg.Kafka.OrderTopic))
			}
		}
		log.Info("Records channel closed")
	}()

	// Khởi động connector
	log.Info("Starting connector...")
	if err := conn.Start(ctx); err != nil {
		log.Fatal("Failed to start connector", logger.Error(err))
	}
	defer func() {
		log.Info("Stopping connector...")
		if err := conn.Stop(ctx); err != nil {
			log.Error("Failed to stop connector", logger.Error(err))
		}
	}()

	log.Info("Pancake connector started successfully, polling continuously...")
	log.Info("Press Ctrl+C to stop")

	// Chạy cho đến khi context bị cancel
	<-ctx.Done()
	log.Info("Pancake connector stopped")
}
