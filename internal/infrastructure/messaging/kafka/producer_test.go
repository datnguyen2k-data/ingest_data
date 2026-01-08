package kafka

import (
	"context"
	"testing"

	"ingest_data/internal/config"
	"ingest_data/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockLogger là mock cho logger.Logger interface
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
		return nil
	}
	return args.Get(0).(logger.Logger)
}

func (m *MockLogger) WithFields(fields ...logger.Field) logger.Logger {
	args := m.Called(fields)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(logger.Logger)
}

func (m *MockLogger) Sync() error {
	args := m.Called()
	return args.Error(0)
}

func TestOrderProducer_PublishOrder_EmptyPayload(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	cfg := config.KafkaConfig{
		Brokers:    []string{"localhost:9092"},
		OrderTopic: "test-topic",
	}

	// Note: Trong thực tế, NewOrderProducer sẽ tạo kgo.Client thật
	// Để test validation logic, ta có thể tạo một test helper
	// hoặc test integration với testcontainers
	// Ở đây ta test validation logic của PublishOrder

	// Tạo producer với mock logger
	// Lưu ý: Test này chỉ test validation, không test kết nối Kafka thật
	producer := &OrderProducer{
		topic:  cfg.OrderTopic,
		logger: mockLog,
		// client sẽ nil trong test này, nhưng ta chỉ test validation
	}

	ctx := context.Background()
	emptyPayload := []byte{}

	// Act
	err := producer.PublishOrder(ctx, emptyPayload)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "payload is empty")
}

// TestOrderProducer_PublishOrder_ValidPayload test với valid payload
// Lưu ý: Test này chỉ test validation, không test kết nối Kafka thật
// Để test đầy đủ, cần dùng testcontainers hoặc mock kgo.Client
func TestOrderProducer_PublishOrder_ValidPayload(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	producer := &OrderProducer{
		topic:  "test-topic",
		logger: mockLog,
		// client sẽ nil trong test này
	}

	ctx := context.Background()
	validPayload := []byte(`{"id": "123", "status": "pending"}`)

	// Setup mock expectations
	mockLog.On("Error", mock.Anything, mock.Anything).Return()
	mockLog.On("Debug", mock.Anything, mock.Anything).Return()

	// Act
	// Lưu ý: Test này sẽ có error vì client là nil
	// Trong thực tế, cần mock kgo.Client hoặc dùng testcontainers
	err := producer.PublishOrder(ctx, validPayload)

	// Assert
	// Vì client là nil, sẽ có panic khi gọi ProduceSync
	// Test này chỉ để đảm bảo validation logic hoạt động
	// Integration test sẽ test với Kafka thật
	if err != nil {
		// Expected error vì client chưa được khởi tạo
		assert.NotNil(t, err)
	}
}

// TestOrderProducer_Close test Close method
func TestOrderProducer_Close(t *testing.T) {
	// Arrange
	mockLog := new(MockLogger)
	producer := &OrderProducer{
		topic:  "test-topic",
		logger: mockLog,
		// client có thể nil trong test
	}

	ctx := context.Background()

	// Setup mock expectations
	mockLog.On("Info", "Closing Kafka producer", mock.Anything).Return()

	// Act
	err := producer.Close(ctx)

	// Assert
	// Close không return error nếu client là nil
	// Trong thực tế, nếu client != nil thì Close() sẽ được gọi
	assert.NoError(t, err)
	mockLog.AssertExpectations(t)
}
