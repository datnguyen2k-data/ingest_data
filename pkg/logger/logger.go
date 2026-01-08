package logger

import (
	"context"
)

// Logger interface để đảm bảo Dependency Inversion Principle (SOLID)
// Các components phụ thuộc vào abstraction này thay vì concrete implementation
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)

	// WithContext trả về logger với context fields
	WithContext(ctx context.Context) Logger

	// WithFields trả về logger với các fields bổ sung
	WithFields(fields ...Field) Logger

	// Sync flushes any buffered log entries
	Sync() error
}

// Field đại diện cho một field trong log entry
type Field struct {
	Key   string
	Value interface{}
}

// Helper functions để tạo Field
func String(key, value string) Field {
	return Field{Key: key, Value: value}
}

func Int(key string, value int) Field {
	return Field{Key: key, Value: value}
}

func Int64(key string, value int64) Field {
	return Field{Key: key, Value: value}
}

func Float64(key string, value float64) Field {
	return Field{Key: key, Value: value}
}

func Bool(key string, value bool) Field {
	return Field{Key: key, Value: value}
}

func Error(err error) Field {
	return Field{Key: "error", Value: err}
}

func Any(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}
