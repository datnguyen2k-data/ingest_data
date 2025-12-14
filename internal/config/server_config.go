package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	App AppConfig
	DB  PostgresConfig
}

type AppConfig struct {
	Name string
	Port int
	Env  string
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

func Load() (*Config, error) {
	// Load .env (không lỗi nếu không có, để dùng cho prod)
	_ = godotenv.Load()

	cfg := &Config{
		App: AppConfig{
			Name: getEnv("APP_NAME", "ingest_data"),
			Env:  getEnv("APP_ENV", "local"),
			Port: getEnvAsInt("APP_PORT", 8030),
		},
		DB: PostgresConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvAsInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", ""),
			DBName:   getEnv("DB_NAME", "postgres"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
			MaxConns: getEnvAsInt("DB_MAX_CONNS", 10),
		},
	}

	return cfg, cfg.validate()
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
	if c.App.Port <= 0 {
		return fmt.Errorf("APP_PORT is invalid")
	}
	if c.DB.Host == "" || c.DB.User == "" || c.DB.DBName == "" {
		return fmt.Errorf("database config is incomplete")
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
