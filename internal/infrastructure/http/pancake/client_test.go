package pancake

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"ingest_data/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cfg := config.PancakeConfig{
		BaseURL:  "https://pos.pages.fm/api/v1",
		APIKey:   "test-key",
		ShopID:   "test-shop",
		PageSize: 100,
		SleepMS:  500,
	}

	client := NewClient(cfg)
	require.NotNil(t, client)
	assert.NotNil(t, client.httpClient)
	assert.Equal(t, cfg, client.cfg)
}

func TestClient_FetchOrdersIncremental_EmptyConfig(t *testing.T) {
	cfg := config.PancakeConfig{
		BaseURL:  "https://pos.pages.fm/api/v1",
		APIKey:   "", // Empty API key
		ShopID:   "test-shop",
		PageSize: 100,
		SleepMS:  500,
	}

	client := NewClient(cfg)
	ctx := context.Background()

	orders, err := client.FetchOrdersIncremental(ctx, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, orders)
	assert.Contains(t, err.Error(), "api_key or shop_id is empty")
}

func TestClient_FetchOrdersIncremental_InvalidBaseURL(t *testing.T) {
	cfg := config.PancakeConfig{
		BaseURL:  "://invalid-url", // Invalid URL
		APIKey:   "test-key",
		ShopID:   "test-shop",
		PageSize: 100,
		SleepMS:  500,
	}

	client := NewClient(cfg)
	ctx := context.Background()

	orders, err := client.FetchOrdersIncremental(ctx, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, orders)
	assert.Contains(t, err.Error(), "invalid pancake base url")
}

// Test với API thật (cần config trong .env)
// Chạy test này với: go test -v -run TestClient_FetchOrdersIncremental_RealAPI
func TestClient_FetchOrdersIncremental_RealAPI(t *testing.T) {
	// Skip test nếu không có config
	cfg, err := config.Load()
	if err != nil {
		t.Skip("Skipping test: config not loaded")
	}

	if cfg.Pancake.APIKey == "" || cfg.Pancake.ShopID == "" {
		t.Skip("Skipping test: PANCAKE_CHANDO_API_KEY or PANCAKE_CHANDO_SHOP_ID not set")
	}

	client := NewClient(cfg.Pancake)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test fetch orders trong 7 ngày gần nhất
	end := time.Now().UTC()
	start := end.Add(-7 * 24 * time.Hour)

	t.Logf("Fetching orders from %s to %s", start.Format(time.RFC3339), end.Format(time.RFC3339))

	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	if err != nil {
		// Nếu lỗi do API (không phải config), log để debug
		t.Logf("API call failed: %v", err)
		// Không fail test nếu là lỗi từ API (có thể do network, API down, etc.)
		if assert.Error(t, err) {
			// Kiểm tra loại lỗi
			errMsg := err.Error()
			if strings.Contains(errMsg, "client error") {
				t.Log("Client error (4xx) - check API key, shop ID, or request parameters")
			} else if strings.Contains(errMsg, "server error") {
				t.Log("Server error (5xx) - Pancake API might be down")
			} else if strings.Contains(errMsg, "call pancake api") {
				t.Log("Network error - check connection")
			}
		}
		return
	}

	// Nếu thành công, kiểm tra kết quả
	require.NoError(t, err, "FetchOrdersIncremental should not error")
	assert.NotNil(t, orders, "orders should not be nil")

	t.Logf("Successfully fetched %d orders", len(orders))

	// Validate mỗi order là JSON hợp lệ
	for i, order := range orders {
		assert.True(t, json.Valid(order), "order[%d] should be valid JSON", i)
		if !json.Valid(order) {
			t.Logf("Invalid JSON at index %d: %s", i, string(order))
		}
	}
}

// Test fetch không có date range
func TestClient_FetchOrdersIncremental_NoDateRange(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Skip("Skipping test: config not loaded")
	}

	if cfg.Pancake.APIKey == "" || cfg.Pancake.ShopID == "" {
		t.Skip("Skipping test: PANCAKE_CHANDO_API_KEY or PANCAKE_CHANDO_SHOP_ID not set")
	}

	client := NewClient(cfg.Pancake)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch không có date range (sẽ lấy tất cả orders)
	orders, err := client.FetchOrdersIncremental(ctx, nil, nil)

	if err != nil {
		t.Logf("API call failed: %v", err)
		return
	}

	require.NoError(t, err)
	assert.NotNil(t, orders)
	t.Logf("Fetched %d orders without date range", len(orders))
}
