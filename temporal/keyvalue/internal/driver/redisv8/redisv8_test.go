package redisv8

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/utils"
	"github.com/TykTechnologies/storage/temporal/types"
)

func TestNewRedisV8(t *testing.T) {
	// Assure a local Redis server is available at this address for this test
	const localRedisAddress = "localhost:6379"

	tests := []struct {
		name      string
		setupConn func() (types.Connector, error)
		wantErr   bool
	}{
		{
			name: "default options",
			setupConn: func() (types.Connector, error) {
				return connector.NewConnector(types.RedisV8Type, types.WithRedisConfig(&types.RedisOptions{
					Addrs: []string{localRedisAddress},
				}))
			},
			wantErr: false,
		},
		{
			name: "invalid connector",
			setupConn: func() (types.Connector, error) {
				return utils.MockConnector{}, nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := tt.setupConn()
			if err != nil {
				t.Errorf("NewRedisV8() error = %v", err)
				return
			}
			r8, err := NewRedisV8(conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRedisV8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
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
