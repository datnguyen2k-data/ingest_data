package pancake

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockOrderFetcher là mock cho OrderFetcher interface
type MockOrderFetcher struct {
	mock.Mock
}

func (m *MockOrderFetcher) FetchOrdersIncremental(ctx context.Context, startDate, endDate *time.Time) ([]json.RawMessage, error) {
	args := m.Called(ctx, startDate, endDate)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]json.RawMessage), args.Error(1)
}

// MockPublisher là mock cho Publisher interface
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) PublishOrder(ctx context.Context, payload []byte) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

func TestService_SyncIncremental_Success(t *testing.T) {
	// Arrange
	mockFetcher := new(MockOrderFetcher)
	mockPublisher := new(MockPublisher)
	service := NewService(mockFetcher, mockPublisher)

	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Tạo mock orders
	order1 := json.RawMessage(`{"id": "1", "status": "pending"}`)
	order2 := json.RawMessage(`{"id": "2", "status": "completed"}`)
	orders := []json.RawMessage{order1, order2}

	// Setup expectations
	mockFetcher.On("FetchOrdersIncremental", ctx, &start, &end).Return(orders, nil)
	// PublishOrder nhận []byte, nhưng order1 là json.RawMessage (alias của []byte)
	// Sử dụng mock.MatchedBy để match bất kỳ []byte nào
	mockPublisher.On("PublishOrder", ctx, mock.MatchedBy(func(payload []byte) bool {
		return len(payload) > 0
	})).Return(nil).Twice()

	// Act
	count, err := service.SyncIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	mockFetcher.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestService_SyncIncremental_FetchError(t *testing.T) {
	// Arrange
	mockFetcher := new(MockOrderFetcher)
	mockPublisher := new(MockPublisher)
	service := NewService(mockFetcher, mockPublisher)

	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	fetchError := errors.New("fetch failed")

	// Setup expectations
	mockFetcher.On("FetchOrdersIncremental", ctx, &start, &end).Return(nil, fetchError)

	// Act
	count, err := service.SyncIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fetch orders")
	assert.Equal(t, 0, count)
	mockFetcher.AssertExpectations(t)
	mockPublisher.AssertNotCalled(t, "PublishOrder", mock.Anything, mock.Anything)
}

func TestService_SyncIncremental_PublishError(t *testing.T) {
	// Arrange
	mockFetcher := new(MockOrderFetcher)
	mockPublisher := new(MockPublisher)
	service := NewService(mockFetcher, mockPublisher)

	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	order1 := json.RawMessage(`{"id": "1", "status": "pending"}`)
	orders := []json.RawMessage{order1}

	publishError := errors.New("publish failed")

	// Setup expectations
	mockFetcher.On("FetchOrdersIncremental", ctx, &start, &end).Return(orders, nil)
	mockPublisher.On("PublishOrder", ctx, mock.MatchedBy(func(payload []byte) bool {
		return len(payload) > 0
	})).Return(publishError)

	// Act
	count, err := service.SyncIncremental(ctx, &start, &end)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publish order")
	assert.Equal(t, 0, count) // Không có order nào được publish thành công
	mockFetcher.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestService_SyncIncremental_InvalidJSON(t *testing.T) {
	// Arrange
	mockFetcher := new(MockOrderFetcher)
	mockPublisher := new(MockPublisher)
	service := NewService(mockFetcher, mockPublisher)

	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	// Invalid JSON và valid JSON
	invalidOrder := json.RawMessage(`{invalid json}`)
	validOrder := json.RawMessage(`{"id": "2", "status": "completed"}`)
	orders := []json.RawMessage{invalidOrder, validOrder}

	// Setup expectations
	mockFetcher.On("FetchOrdersIncremental", ctx, &start, &end).Return(orders, nil)
	// Invalid JSON sẽ bị skip, chỉ publish valid order
	mockPublisher.On("PublishOrder", ctx, mock.MatchedBy(func(payload []byte) bool {
		return len(payload) > 0
	})).Return(nil)

	// Act
	count, err := service.SyncIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Chỉ có 1 valid order được publish
	mockFetcher.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestService_SyncIncremental_EmptyOrders(t *testing.T) {
	// Arrange
	mockFetcher := new(MockOrderFetcher)
	mockPublisher := new(MockPublisher)
	service := NewService(mockFetcher, mockPublisher)

	ctx := context.Background()
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	orders := []json.RawMessage{}

	// Setup expectations
	mockFetcher.On("FetchOrdersIncremental", ctx, &start, &end).Return(orders, nil)

	// Act
	count, err := service.SyncIncremental(ctx, &start, &end)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	mockFetcher.AssertExpectations(t)
	mockPublisher.AssertNotCalled(t, "PublishOrder", mock.Anything, mock.Anything)
}

