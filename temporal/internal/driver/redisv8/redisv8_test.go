package redisv8

import (
	"context"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/TykTechnologies/storage/temporal/utils"
)

func TestNewRedisV8(t *testing.T) {
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
			r8 := NewRedisV8(tt.opts)

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

func TestGetRedisAddrs(t *testing.T) {
	tests := []struct {
		name string
		opts types.RedisOptions
		want []string
	}{
		{
			name: "With Addrs",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
		},
		{
			name: "With Hosts map",
			opts: types.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379", "127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6380"},
		},
		{
			name: "With Host and Port",
			opts: types.RedisOptions{
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.1",
					Port: 6379,
				},
			},
			want: []string{"127.0.0.1:6379"},
		},
		{
			name: "With empty options",
			opts: types.RedisOptions{},
			want: []string{},
		},
		{
			name: "With Addrs and Hosts map",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				Hosts: map[string]string{"127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Hosts map
		},
		{
			name: "With Addrs and Host/Port",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.2",
					Port: 6380,
				},
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Host/Port
		},
		{
			name: "With Hosts map and Host/Port",
			opts: types.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379"},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.2",
					Port: 6380,
				},
			},
			want: []string{"127.0.0.1:6379"}, // Hosts map takes priority over Host/Port
		},
		{
			name: "With all empty values",
			opts: types.RedisOptions{
				Hosts: map[string]string{},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "",
					Port: 0,
				},
			},
			want: []string{},
		},
		{
			name: "With Port only",
			opts: types.RedisOptions{
				BaseStorageOptions: types.BaseStorageOptions{
					Port: 6379,
				},
			},
			want: []string{":6379"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := utils.GetRedisAddrs(&tt.opts)
			equals := utils.CompareUnorderedSlices(got, tt.want)
			if !equals {
				t.Errorf("getRedisAddrs() = %v, want %v", got, tt.want)
			}
		})
	}
}
