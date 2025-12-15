package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	App    AppConfig
	Server ServerConfig
	DB     PostgresConfig
	Kafka  KafkaConfig
}

type AppConfig struct {
	Name string
	Env  string
}

type ServerConfig struct {
	Host string
	Port int
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
	MaxConns int
}

type KafkaConfig struct {
	Brokers       []string
	OrderTopic    string
	ConsumerGroup string
}

func Load() (*Config, error) {
	_ = godotenv.Load(".env")

	cfg := &Config{
		App: AppConfig{
			Name: getEnv("APP_NAME", "ingest_data"),
			Env:  getEnv("APP_ENV", "local"),
		},
		Server: ServerConfig{
			Host: getEnv("HTTP_HOST", "0.0.0.0"),
			Port: getEnvAsInt("HTTP_PORT", 8030),
		},
		DB: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnvAsInt("POSTGRES_PORT", 5432),
			User:     getEnv("POSTGRES_USER", "postgres"),
			Password: getEnv("POSTGRES_PASSWORD", ""),
			DBName:   getEnv("POSTGRES_DB", "postgres"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
			MaxConns: getEnvAsInt("DB_MAX_CONNS", 10),
		},
		Kafka: KafkaConfig{
			Brokers:       splitAndTrim(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
			OrderTopic:    getEnv("KAFKA_ORDER_TOPIC", "orders"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "ingest-data"),
		},
	}

	return cfg, cfg.validate()
}

func (s ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User,
		p.Password,
		p.Host,
		p.Port,
		p.DBName,
		p.SSLMode,
	)
}

/* ================= helpers ================= */

func (c *Config) validate() error {
	if c.Server.Port <= 0 {
		return fmt.Errorf("HTTP_PORT is invalid")
	}
	if c.DB.Host == "" || c.DB.User == "" || c.DB.DBName == "" {
		return fmt.Errorf("database config is incomplete")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers is empty")
	}
	return nil
}

func getEnv(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if val := strings.TrimSpace(p); val != "" {
			out = append(out, val)
		}
	}
	return out
}
