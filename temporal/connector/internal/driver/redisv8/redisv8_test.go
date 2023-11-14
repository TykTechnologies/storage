package redisv8

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/temporal/connector/types"
)

func TestNewConnector(t *testing.T) {
	const localRedisAddress = "localhost:6379"

	tests := []struct {
		name             string
		opts             types.Option
		expectConnection bool // Expect a successful connection and PING command
		expectedError    error
	}{
		{
			name: "default options",
			opts: types.WithRedisConfig(&types.RedisOptions{
				Addrs: []string{"localhost:6379"},
			}),
			expectConnection: true,
		},
		{
			name: "custom pool size and timeout",
			opts: types.WithRedisConfig(&types.RedisOptions{
				Addrs:     []string{localRedisAddress},
				MaxActive: 10,
				Timeout:   10,
			}),
			expectConnection: true,
		},
		{
			name: "invalid address",
			opts: types.WithRedisConfig(&types.RedisOptions{
				Addrs: []string{"invalid:asdas"},
			}),
			expectConnection: false,
		},
		{
			name: "use SSL",
			opts: types.WithRedisConfig(&types.RedisOptions{
				Addrs:   []string{localRedisAddress},
				UseSSL:  true,
				Timeout: 1,
			}),
			expectConnection: false,
		},
		{
			name:             "invalid configuration",
			opts:             types.WithNoopConfig(),
			expectedError:    types.ErrInvalidOptionsType,
			expectConnection: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := NewConnector(tt.opts)
			assert.Equal(t, tt.expectedError, err)
			if err != nil {
				return
			}

			if tt.expectConnection {
				err := conn.Ping(context.TODO())
				assert.NoError(t, err)
			}
		})
	}
}

func TestAs(t *testing.T) {
	conn, err := NewConnector(types.WithRedisConfig(&types.RedisOptions{
		Addrs: []string{"localhost:6379"},
	}))

	assert.Nil(t, err)

	var client redis.UniversalClient
	isClient := conn.As(&client)
	assert.True(t, isClient)

	err = conn.Ping(context.Background())
	assert.Nil(t, err)

	_, err = client.Ping(context.Background()).Result()
	assert.Nil(t, err)

	var client2 struct{}
	isClient = conn.As(&client2)
	assert.False(t, isClient)
}
