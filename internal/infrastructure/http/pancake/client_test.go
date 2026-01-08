package pancake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockLogger cho testing
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debug(msg string, fields ...logger.Field) {
	m.Called(msg, fields)
}

func (m *MockLogger) Info(msg string, fields ...logger.Field) {
	m.Called(msg, fields)
}

func (m *MockLogger) Warn(msg string, fields ...logger.Field) {
	m.Called(msg, fields)
}

func (m *MockLogger) Error(msg string, fields ...logger.Field) {
	m.Called(msg, fields)
}

func (m *MockLogger) Fatal(msg string, fields ...logger.Field) {
	m.Called(msg, fields)
}

func (m *MockLogger) WithContext(ctx context.Context) logger.Logger {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return m
	}
	return args.Get(0).(logger.Logger)
}

func (m *MockLogger) WithFields(fields ...logger.Field) logger.Logger {
	args := m.Called(fields)
	if args.Get(0) == nil {
		return m
	}
	return args.Get(0).(logger.Logger)
}

func (m *MockLogger) Sync() error {
	args := m.Called()
	return args.Error(0)
}

func TestClient_FetchOrdersIncremental_Success(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	
	// Tạo mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": []json.RawMessage{
				json.RawMessage(`{"id": "1", "status": "pending"}`),
				json.RawMessage(`{"id": "2", "status": "completed"}`),
			},
			"total_pages": 1,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := config.PancakeConfig{
		BaseURL:  server.URL,
		APIKey:   "test-api-key",
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Setup mock expectations
	mockLog.On("Info", mock.Anything, mock.Anything).Return()
	mockLog.On("Warn", mock.Anything, mock.Anything).Return()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, orders, 2)
	mockLog.AssertExpectations(t)
}

func TestClient_FetchOrdersIncremental_EmptyAPIKey(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	cfg := config.PancakeConfig{
		BaseURL:  "https://api.example.com",
		APIKey:   "", // Empty API key
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "api_key or shop_id is empty")
	assert.Nil(t, orders)
}

func TestClient_FetchOrdersIncremental_EmptyShopID(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	cfg := config.PancakeConfig{
		BaseURL:  "https://api.example.com",
		APIKey:   "test-api-key",
		ShopID:   "", // Empty Shop ID
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "api_key or shop_id is empty")
	assert.Nil(t, orders)
}

func TestClient_FetchOrdersIncremental_HTTPError(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	
	// Tạo mock HTTP server trả về error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	cfg := config.PancakeConfig{
		BaseURL:  server.URL,
		APIKey:   "test-api-key",
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Setup mock expectations
	mockLog.On("Info", mock.Anything, mock.Anything).Return()
	mockLog.On("Error", mock.Anything, mock.Anything).Return()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server error")
	assert.Nil(t, orders)
	mockLog.AssertExpectations(t)
}

func TestClient_FetchOrdersIncremental_InvalidJSON(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	
	// Tạo mock HTTP server trả về invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	cfg := config.PancakeConfig{
		BaseURL:  server.URL,
		APIKey:   "test-api-key",
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Setup mock expectations
	mockLog.On("Info", mock.Anything, mock.Anything).Return()
	mockLog.On("Error", mock.Anything, mock.Anything).Return()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
	assert.Nil(t, orders)
	mockLog.AssertExpectations(t)
}

func TestClient_FetchOrdersIncremental_EmptyData(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	
	// Tạo mock HTTP server trả về empty data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data":       []json.RawMessage{},
			"total_pages": 1,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := config.PancakeConfig{
		BaseURL:  server.URL,
		APIKey:   "test-api-key",
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  100,
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Setup mock expectations
	mockLog.On("Info", mock.Anything, mock.Anything).Return()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, orders, 0)
	mockLog.AssertExpectations(t)
}

func TestClient_FetchOrdersIncremental_MultiplePages(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	pageCount := 0
	
	// Tạo mock HTTP server với pagination
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		response := map[string]interface{}{
			"data": []json.RawMessage{
				json.RawMessage(`{"id": "` + string(rune(pageCount)) + `", "status": "pending"}`),
			},
			"total_pages": 2,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := config.PancakeConfig{
		BaseURL:  server.URL,
		APIKey:   "test-api-key",
		ShopID:   "test-shop-id",
		PageSize: 500,
		SleepMS:  10, // Giảm sleep time cho test
	}

	client := NewClient(cfg, mockLog)
	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Setup mock expectations
	mockLog.On("Info", mock.Anything, mock.Anything).Return()

	// Act
	orders, err := client.FetchOrdersIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(orders), 1) // Ít nhất 1 order từ mỗi page
	mockLog.AssertExpectations(t)
}
