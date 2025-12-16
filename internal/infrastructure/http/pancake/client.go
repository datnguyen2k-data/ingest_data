package pancake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"ingest_data/internal/config"
)

type Client struct {
	httpClient *http.Client
	cfg        config.PancakeConfig
}

func NewClient(cfg config.PancakeConfig) *Client {
	return &Client{
		cfg: cfg,
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

// FetchOrdersIncremental tương tự Python: lấy theo updated_at, phân trang, sleep giữa các page.
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
		log.Printf("[Pancake API] Page %d: Calling %s", page, logURL)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			log.Printf("[Pancake API] Request failed: %v", err)
			return nil, fmt.Errorf("call pancake api: %w", err)
		}

		// Đọc response body (cần đọc trước khi check status để có thể log)
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if resp.Body != nil {
			resp.Body.Close()
		}

		// Log response status
		log.Printf("[Pancake API] Page %d: Response status %d", page, resp.StatusCode)

		// Xử lý error theo status code
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			errorMsg := fmt.Sprintf("pancake api status %d", resp.StatusCode)
			if readErr == nil && len(bodyBytes) > 0 {
				errorMsg = fmt.Sprintf("pancake api status %d: %s", resp.StatusCode, string(bodyBytes))
				log.Printf("[Pancake API] Error response body: %s", string(bodyBytes))
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
			log.Printf("[Pancake API] Failed to decode response: %v", err)
			log.Printf("[Pancake API] Response body: %s", string(bodyBytes))
			return nil, fmt.Errorf("decode response: %w", err)
		}

		// Log thông tin page
		log.Printf("[Pancake API] Page %d: received %d orders, total pages: %d",
			page, len(body.Data), body.TotalPages)

		if len(body.Data) == 0 {
			log.Printf("[Pancake API] Page %d: no more data, stopping", page)
			break
		}

		allOrders = append(allOrders, body.Data...)

		if body.TotalPages > 0 {
			totalPages = body.TotalPages
		}
		page++

		select {
		case <-ctx.Done():
			log.Printf("[Pancake API] Context cancelled, stopping at page %d", page)
			return allOrders, ctx.Err()
		case <-time.After(sleep):
		}
	}

	log.Printf("[Pancake API] Completed: fetched %d orders from %d page(s)", len(allOrders), page-1)
	return allOrders, nil
}
