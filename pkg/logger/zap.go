package logger

import (
	"context"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapLogger là implementation của Logger interface sử dụng zap
type ZapLogger struct {
	logger *zap.Logger
}

// NewZapLogger tạo logger mới với zap
// env: "development" hoặc "production"
func NewZapLogger(env string) (Logger, error) {
	var config zap.Config
	
	if env == "production" || env == "prod" {
		config = zap.NewProductionConfig()
		config.EncoderConfig.TimeKey = "timestamp"
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	// Set output to stdout
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	zapLogger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &ZapLogger{logger: zapLogger}, nil
}

// NewZapLoggerFromEnv tạo logger từ environment variable APP_ENV
func NewZapLoggerFromEnv() (Logger, error) {
	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "development"
	}
	return NewZapLogger(env)
}

// Debug logs a message at DebugLevel
func (l *ZapLogger) Debug(msg string, fields ...Field) {
	l.logger.Debug(msg, convertFields(fields)...)
}

// Info logs a message at InfoLevel
func (l *ZapLogger) Info(msg string, fields ...Field) {
	l.logger.Info(msg, convertFields(fields)...)
}

// Warn logs a message at WarnLevel
func (l *ZapLogger) Warn(msg string, fields ...Field) {
	l.logger.Warn(msg, convertFields(fields)...)
}

// Error logs a message at ErrorLevel
func (l *ZapLogger) Error(msg string, fields ...Field) {
	l.logger.Error(msg, convertFields(fields)...)
}

// Fatal logs a message at FatalLevel then calls os.Exit(1)
func (l *ZapLogger) Fatal(msg string, fields ...Field) {
	l.logger.Fatal(msg, convertFields(fields)...)
}

// WithContext trả về logger với context fields
func (l *ZapLogger) WithContext(ctx context.Context) Logger {
	// Có thể extract request ID, user ID từ context nếu cần
	// Hiện tại chỉ trả về logger hiện tại
	return l
}

// WithFields trả về logger với các fields bổ sung
func (l *ZapLogger) WithFields(fields ...Field) Logger {
	return &ZapLogger{
		logger: l.logger.With(convertFields(fields)...),
	}
}

// Sync flushes any buffered log entries
func (l *ZapLogger) Sync() error {
	return l.logger.Sync()
}

// convertFields chuyển đổi Field của chúng ta sang zap.Field
func convertFields(fields []Field) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		switch v := f.Value.(type) {
		case string:
			zapFields = append(zapFields, zap.String(f.Key, v))
		case int:
			zapFields = append(zapFields, zap.Int(f.Key, v))
		case int64:
			zapFields = append(zapFields, zap.Int64(f.Key, v))
		case float64:
			zapFields = append(zapFields, zap.Float64(f.Key, v))
		case bool:
			zapFields = append(zapFields, zap.Bool(f.Key, v))
		case error:
			zapFields = append(zapFields, zap.Error(v))
		default:
			zapFields = append(zapFields, zap.Any(f.Key, v))
		}
	}
	return zapFields
}

