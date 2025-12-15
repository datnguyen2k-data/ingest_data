package postgres

import (
	"context"
	"testing"
	"time"

	"ingest_data/internal/config"

	"github.com/stretchr/testify/require"
)

func TestNewPool_WithEnv(t *testing.T) {
	// Load config (.env sẽ được load bên trong)
	cfg, err := config.Load()
	require.NoError(t, err, "load config failed")

	// Create DB pool
	pool, err := NewPool(cfg.DB)
	require.NoError(t, err, "NewPool failed")
	require.NotNil(t, pool, "pool should not be nil")

	defer pool.Close()

	// Verify connection works
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pool.Ping(ctx)
	require.NoError(t, err, "ping database failed")
}
