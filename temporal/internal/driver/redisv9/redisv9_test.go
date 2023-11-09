package redisv9

import (
	"context"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
)

func TestNewRedisV9(t *testing.T) {
	// Assure a local Redis server is available at this address for this test
	const localRedisAddress = "localhost:6379"

	tests := []struct {
		name             string
		opts             *types.ClientOpts
		expectConnection bool // Expect a successful connection and PING command
		expectedTimeout  time.Duration
	}{
		{
			name: "default options",
			opts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					Addrs: []string{localRedisAddress},
				},
			},
			expectConnection: true,
		},
		{
			name: "custom pool size and timeout",
			opts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					Addrs:     []string{localRedisAddress},
					MaxActive: 10,
					BaseStorageOptions: types.BaseStorageOptions{
						Timeout: 10,
					},
				},
			},
			expectConnection: true,
			expectedTimeout:  10 * time.Second,
		},
		{
			name: "invalid address",
			opts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					Addrs: []string{"invalid:asdas"},
				},
			},
		},
		{
			name: "use SSL",
			opts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					Addrs: []string{localRedisAddress},
					BaseStorageOptions: types.BaseStorageOptions{
						UseSSL:  true,
						Timeout: 1,
					},
				},
			},
			expectConnection: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r8 := NewRedisV9(tt.opts)

			if tt.expectConnection {
				if r8.client == nil {
					t.Errorf("Expected a Redis client, got nil")
					return
				}

				pong, err := r8.client.Ping(context.Background()).Result()
				if err != nil || pong != "PONG" {
					t.Errorf("Expected PONG, got %v, error: %v", pong, err)
				}
			} else {
				if r8 != nil {
					// Attempt to use the client to ensure it does not work as expected (since we expect no connection)
					_, err := r8.client.Ping(context.Background()).Result()
					if err == nil {
						t.Errorf("Expected not to connect to Redis, but connection was successful")
					}
				}
				return
			}

			if r8.client != nil {
				if err := r8.client.Close(); err != nil {
					t.Errorf("Failed to close Redis client: %v", err)
				}
			}
		})
	}
}
