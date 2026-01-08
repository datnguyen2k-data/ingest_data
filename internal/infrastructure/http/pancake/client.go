package pancake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"
)

type Client struct {
	httpClient *http.Client
	cfg        config.PancakeConfig
	logger     logger.Logger
}

// NewClient tạo client mới với logger được inject (DI)
func NewClient(cfg config.PancakeConfig, log logger.Logger) *Client {
	return &Client{
		cfg: cfg,
		logger: log,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
			},
		},
	}
}

type ordersResponse struct {
	Data       []json.RawMessage `json:"data"`
	TotalPages int               `json:"total_pages"`
}

// FetchOrdersIncremental : lấy theo updated_at, phân trang, sleep giữa các page.
func (c *Client) FetchOrdersIncremental(
	ctx context.Context,
	startDate *time.Time,
	endDate *time.Time,
) ([]json.RawMessage, error) {
	if c.cfg.APIKey == "" || c.cfg.ShopID == "" {
		return nil, fmt.Errorf("pancake api_key or shop_id is empty")
	}

	allOrders := make([]json.RawMessage, 0)
	page := 1
	totalPages := 1
	pageSize := c.cfg.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	sleep := time.Duration(c.cfg.SleepMS) * time.Millisecond
	if sleep <= 0 {
		sleep = time.Second
	}

	base, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid pancake base url: %w", err)
	}

	for page <= totalPages {
		u := *base
		u.Path = fmt.Sprintf("%s/shops/%s/orders", base.Path, c.cfg.ShopID)

		q := u.Query()
		q.Set("api_key", c.cfg.APIKey)
		q.Set("page_size", fmt.Sprintf("%d", pageSize))
		q.Set("page_number", fmt.Sprintf("%d", page))
		q.Set("option_sort", "updated_at_desc")

		if startDate != nil && endDate != nil {
			q.Set("startDateTime", fmt.Sprintf("%d", startDate.Unix()))
			q.Set("endDateTime", fmt.Sprintf("%d", endDate.Unix()))
			q.Set("updateStatus", "updated_at")
		}
		u.RawQuery = q.Encode()

		// Log URL (ẩn API key để bảo mật)
		logURL := u.String()
		if c.cfg.APIKey != "" {
			logURL = strings.ReplaceAll(logURL, c.cfg.APIKey, "***")
		}
		c.logger.Info("Calling Pancake API",
			logger.Int("page", page),
			logger.String("url", logURL))

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.logger.Error("Pancake API request failed",
				logger.Int("page", page),
				logger.Error(err))
			return nil, fmt.Errorf("call pancake api: %w", err)
		}

		// Đọc response body (cần đọc trước khi check status để có thể log)
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if resp.Body != nil {
			resp.Body.Close()
		}

		// Log response status
		c.logger.Info("Pancake API response received",
			logger.Int("page", page),
			logger.Int("status_code", resp.StatusCode))

		// Xử lý error theo status code
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			errorMsg := fmt.Sprintf("pancake api status %d", resp.StatusCode)
			if readErr == nil && len(bodyBytes) > 0 {
				errorMsg = fmt.Sprintf("pancake api status %d: %s", resp.StatusCode, string(bodyBytes))
				c.logger.Error("Pancake API error response",
					logger.Int("page", page),
					logger.Int("status_code", resp.StatusCode),
					logger.String("response_body", string(bodyBytes)))
			}

			// Phân loại lỗi
			switch {
			case resp.StatusCode >= 400 && resp.StatusCode < 500:
				// 4xx: Client errors (bad request, unauthorized, not found, etc.)
				return nil, fmt.Errorf("client error - %s", errorMsg)
			case resp.StatusCode >= 500:
				// 5xx: Server errors
				return nil, fmt.Errorf("server error - %s", errorMsg)
			default:
				// Các status code khác (1xx, 3xx không mong đợi)
				return nil, fmt.Errorf("unexpected status - %s", errorMsg)
			}
		}

		// Parse response body
		if readErr != nil {
			return nil, fmt.Errorf("read response body: %w", readErr)
		}

		var body ordersResponse
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			c.logger.Error("Failed to decode Pancake API response",
				logger.Int("page", page),
				logger.String("response_body", string(bodyBytes)),
				logger.Error(err))
			return nil, fmt.Errorf("decode response: %w", err)
		}

		// Log thông tin page
		c.logger.Info("Pancake API page received",
			logger.Int("page", page),
			logger.Int("orders_count", len(body.Data)),
			logger.Int("total_pages", body.TotalPages))

		if len(body.Data) == 0 {
			c.logger.Info("No more data from Pancake API, stopping",
				logger.Int("page", page))
			break
		}

		allOrders = append(allOrders, body.Data...)

		if body.TotalPages > 0 {
			totalPages = body.TotalPages
		}
		page++

		select {
		case <-ctx.Done():
			c.logger.Warn("Context cancelled, stopping Pancake API fetch",
				logger.Int("page", page))
			return allOrders, ctx.Err()
		case <-time.After(sleep):
		}
	}

	c.logger.Info("Completed fetching orders from Pancake API",
		logger.Int("total_orders", len(allOrders)),
		logger.Int("total_pages", page-1))
	return allOrders, nil
}
