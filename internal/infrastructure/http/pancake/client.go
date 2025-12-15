package pancake

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
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

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("call pancake api: %w", err)
		}

		if resp.Body != nil {
			defer resp.Body.Close()
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("pancake api status %d", resp.StatusCode)
		}

		var body ordersResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}

		if len(body.Data) == 0 {
			break
		}

		allOrders = append(allOrders, body.Data...)

		if body.TotalPages > 0 {
			totalPages = body.TotalPages
		}
		page++

		select {
		case <-ctx.Done():
			return allOrders, ctx.Err()
		case <-time.After(sleep):
		}
	}

	return allOrders, nil
}
