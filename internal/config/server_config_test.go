package config

import (
	"testing"
    "github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)

func TestServerConfig_Address(t *testing.T) {
	tests := []struct {
		name   string
		server ServerConfig
		want   string
	}{
		{
			name: "localhost default port",
			server: ServerConfig{
				Host: "localhost",
				Port: 8030,
			},
			want: "localhost:8030",
		},
		{
			name: "bind all interfaces",
			server: ServerConfig{
				Host: "0.0.0.0",
				Port: 8080,
			},
			want: "0.0.0.0:8080",
		},
		{
			name: "custom host and port",
			server: ServerConfig{
				Host: "api.internal",
				Port: 9000,
			},
			want: "api.internal:9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			address := tt.server.Address()
			assert.Equal(t, tt.want, address)
		})
	}
}

func TestLoad_PrintDBConfig(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	t.Logf("DB Host: %s", cfg.DB.Host)
	t.Logf("DB Port: %d", cfg.DB.Port)
	t.Logf("DB User: %s", cfg.DB.User)
	t.Logf("DB Name: %s", cfg.DB.DBName)
}
