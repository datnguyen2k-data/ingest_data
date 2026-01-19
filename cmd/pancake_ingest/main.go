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
	"sync"
	"syscall"
	"time"

	"ingest_data/internal/config"
	"ingest_data/internal/infrastructure/encoding/avro"
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
	if cfg.Kafka.SchemaRegistryURL == "" {
		log.Fatal("KAFKA_SCHEMA_REGISTRY_URL is empty")
	}
	if cfg.Pancake.APIKey == "" || cfg.Pancake.ShopID == "" {
		log.Fatal("PANCAKE_CHANDO_API_KEY or PANCAKE_CHANDO_SHOP_ID is empty")
	}

	log.Info("Starting Pancake ingestion service with connector",
		logger.String("app_name", cfg.App.Name),
		logger.String("env", cfg.App.Env),
		logger.Any("kafka_brokers", cfg.Kafka.Brokers),
		logger.String("kafka_topic", cfg.Kafka.OrderTopic),
		logger.String("schema_registry", cfg.Kafka.SchemaRegistryURL),
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

	// Tạo Kafka producer (Confluent)
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

	// Tạo channel để nhận records với buffer lớn cho high throughput
	recordsChan := make(chan []connector.Record, 1000)

	// Tạo connector config
	connectorConfig := connector.PollingConfig{
		ConnectorConfig: connector.ConnectorConfig{
			ConnectorClass: "PancakeHTTPPollingConnector",
			TopicName:      cfg.Kafka.OrderTopic,
			TasksMax:       1,
			HTTPConfig: connector.HTTPClientConfig{
				Timeout:               30 * time.Second,
				MaxIdleConns:          1000,
				MaxIdleConnsPerHost:   100,
				RetryMaxAttempts:      3,
				RetryBackoff:          1 * time.Second,
				MaxConcurrentRequests: 50,
				RequestQueueSize:      1000,
				BatchSize:             1000,
				BatchTimeout:          100 * time.Millisecond,
			},
			OffsetConfig: connector.OffsetConfig{
				Mode:         connector.OffsetModeTimestamp,
				InitialValue: time.Now().UTC().Add(-timeWindow),
			},
		},
		URL:          baseURL.String(),
		PollInterval: pollInterval,

		// RequestBuilder: Xây dựng request cho Pancake API
		RequestBuilder: func(offset connector.Offset) (*connector.HTTPRequest, error) {
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

			startUnix := startTime.Unix()
			endUnix := endTime.Unix()

			u := *baseURL
			u.Path = fmt.Sprintf("%s/shops/%s/orders", baseURL.Path, cfg.Pancake.ShopID)

			q := u.Query()
			q.Set("api_key", cfg.Pancake.APIKey)
			q.Set("startDateTime", fmt.Sprintf("%d", startUnix))
			q.Set("endDateTime", fmt.Sprintf("%d", endUnix))
			q.Set("updateStatus", "inserted_at")
			q.Set("page_size", fmt.Sprintf("%d", cfg.Pancake.PageSize))
			q.Set("page_number", "1")
			q.Set("option_sort", "updated_at_desc")
			u.RawQuery = q.Encode()

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
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				errorMsg := fmt.Sprintf("pancake api status %d: %s", resp.StatusCode, string(resp.Body))
				log.Error("Pancake API error response",
					logger.Int("status_code", resp.StatusCode),
					logger.String("response_body", string(resp.Body)))
				return nil, fmt.Errorf("pancake api error: %s", errorMsg)
			}

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

			// Helper to process items -> Records (using Struct)
			processItems := func(items []json.RawMessage) ([]connector.Record, error) {
				batch := make([]connector.Record, 0, len(items))
				for _, item := range items {
					if !json.Valid(item) {
						continue
					}
					var rawMap map[string]interface{}
					if err := json.Unmarshal(item, &rawMap); err != nil {
						log.Warn("Failed to unmarshal item to map", logger.Error(err))
						continue
					}

					// Map directly to Struct
					pancakeOrder, err := avro.ToPancakeOrderStruct(rawMap)
					if err != nil {
						log.Warn("Failed to map item to struct", logger.Error(err))
						continue
					}

					batch = append(batch, connector.Record{
						Value:     pancakeOrder, // Value is now *PancakeOrderStruct
						Topic:     cfg.Kafka.OrderTopic,
						Timestamp: time.Now().UTC(),
					})
				}
				return batch, nil
			}

			// Process first page
			firstBatch, _ := processItems(body.Data)
			if len(firstBatch) > 0 {
				select {
				case recordsChan <- firstBatch:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			// Fetch subsequent pages
			if body.TotalPages > 1 {
				reqURL, err := url.Parse(resp.Request.URL)
				if err != nil {
					log.Error("Failed to parse request URL", logger.Error(err))
					return nil, nil
				}

				httpClient := &http.Client{Timeout: 30 * time.Second}

				for page := 2; page <= body.TotalPages; page++ {
					q := reqURL.Query()
					q.Set("page_number", fmt.Sprintf("%d", page))
					reqURL.RawQuery = q.Encode()

					log.Debug("Fetching additional page",
						logger.Int("page", page),
						logger.Int("total_pages", body.TotalPages))

					req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
					if err != nil {
						continue
					}

					pageResp, err := httpClient.Do(req)
					if err != nil {
						log.Error("Failed to fetch page", logger.Int("page", page), logger.Error(err))
						continue
					}

					pageBodyBytes, err := io.ReadAll(pageResp.Body)
					pageResp.Body.Close()
					if err != nil {
						continue
					}

					if pageResp.StatusCode < 200 || pageResp.StatusCode >= 300 {
						continue
					}

					var pageBody struct {
						Data []json.RawMessage `json:"data"`
					}
					if err := json.Unmarshal(pageBodyBytes, &pageBody); err != nil {
						continue
					}

					log.Info("Pancake API page received",
						logger.Int("page", page),
						logger.Int("orders_count", len(pageBody.Data)))

					batch, _ := processItems(pageBody.Data)
					if len(batch) > 0 {
						select {
						case recordsChan <- batch:
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(time.Duration(cfg.Pancake.SleepMS) * time.Millisecond):
						}
					}
				}
			}

			return nil, nil
		},

		ErrorHandler: func(err error) error {
			log.Error("Pancake connector error", logger.Error(err))
			return nil
		},
	}

	// Tạo connector
	conn, err := connector.NewHTTPPollingConnector(connectorConfig)
	if err != nil {
		log.Fatal("Failed to create connector", logger.Error(err))
	}

	tasks := conn.GetTasks()
	if len(tasks) > 0 {
		tasks[0].SetRecordsChannel(recordsChan)
	}

	// Worker pool (pass struct directly to producer)
	numWorkers := 10
	log.Info("Starting worker pool for Kafka publishing",
		logger.Int("num_workers", numWorkers),
		logger.Int("channel_buffer", cap(recordsChan)))

	var wg sync.WaitGroup
	totalPublished := int64(0)
	totalErrors := int64(0)
	var statsMu sync.RWMutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerCount := 0
			workerErrors := 0

			for records := range recordsChan {
				for _, record := range records {
					// record.Value is *PancakeOrderStruct
					if err := producer.PublishOrder(ctx, record.Value); err != nil {
						workerErrors++
						statsMu.Lock()
						totalErrors++
						statsMu.Unlock()
						log.Error("Failed to publish to Kafka",
							logger.Int("worker_id", workerID),
							logger.Error(err))
						continue
					}
					workerCount++
				}
				statsMu.Lock()
				totalPublished += int64(len(records))
				statsMu.Unlock()

				if len(records) > 0 {
					log.Debug("Worker published batch",
						logger.Int("worker_id", workerID),
						logger.Int("batch_size", len(records)))
				}
			}
		}(i)
	}

	// Background stats log (same as before) ...
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				statsMu.RLock()
				pub := totalPublished
				errs := totalErrors
				statsMu.RUnlock()
				log.Info("Publishing stats",
					logger.Int64("total_published", pub),
					logger.Int64("total_errors", errs))
			}
		}
	}()

	go func() {
		wg.Wait()
		log.Info("All workers stopped")
	}()

	log.Info("Starting connector...")
	if err := conn.Start(ctx); err != nil {
		log.Fatal("Failed to start connector", logger.Error(err))
	}
	defer func() {
		conn.Stop(ctx)
	}()

	log.Info("Pancake connector started successfully, polling continuously...")
	<-ctx.Done()
	log.Info("Pancake connector stopped")
}
