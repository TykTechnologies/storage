package connector

import (
	"context"
	"os"
	"testing"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/temporal/model"
)

func checkTLS(t *testing.T) *model.TLS {
	t.Helper()

	var tlsConfig *model.TLS
	if os.Getenv("TEST_ENABLE_TLS") == "true" {
		tlsConfig.Enable = true

		tlsConfig.CertFile = os.Getenv("TEST_TLS_CERT_FILE")
		tlsConfig.KeyFile = os.Getenv("TEST_TLS_KEY_FILE")
		tlsConfig.CAFile = os.Getenv("TEST_TLS_CA_FILE")
		tlsConfig.InsecureSkipVerify = os.Getenv("TEST_TLS_INSECURE_SKIP_VERIFY") == "true"
	}

	return tlsConfig
}

func TestNewConnector(t *testing.T) {
	tests := []struct {
		name        string
		typ         string
		opts        []model.Option
		expectedErr error
	}{
		{
			name:        "default",
			typ:         "",
			opts:        []model.Option{},
			expectedErr: temperr.InvalidHandlerType,
		},
		{
			name: "redisv8_with_config",
			typ:  model.RedisV8Type,
			opts: []model.Option{WithRedisConfig(&model.RedisOptions{
				Addrs: []string{"localhost:6379"},
			})},
			expectedErr: nil,
		},
		{
			name:        "redisv8_with_noop_config",
			typ:         model.RedisV8Type,
			opts:        []model.Option{model.WithNoopConfig()},
			expectedErr: temperr.InvalidOptionsType,
		},
		{
			name: "redisv8_with_multiple_opts",
			typ:  model.RedisV8Type,
			opts: []model.Option{WithRedisConfig(&model.RedisOptions{
				Addrs: []string{"localhost:6379"},
			}), model.WithRetries(&model.RetryOptions{
				MaxRetries: 3,
			})},
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tlsConfig := checkTLS(t)
			if tlsConfig != nil {
				tt.opts = append(tt.opts, model.WithTLS(tlsConfig))
			}

			connector, err := NewConnector(tt.typ, tt.opts...)
			assert.Equal(t, tt.expectedErr, err)

			if tt.expectedErr == nil {
				assert.True(t, connector != nil)
			}
		})
	}
}

func TestNewConnector_WithOnConnect(t *testing.T) {
	tlsConfig := checkTLS(t)
	t.Run("redisv8_with_on_connect", func(t *testing.T) {
		var called bool
		onConnect := func(ctx context.Context) error {
			called = true
			return nil
		}
		addrs := os.Getenv("TEST_REDIS_ADDRS")
		if addrs == "" {
			addrs = "localhost:6379"
		}

		connector, err := NewConnector(model.RedisV8Type, WithRedisConfig(&model.RedisOptions{
			Addrs: []string{addrs},
		}), model.WithTLS(tlsConfig), model.WithOnConnect(onConnect))
		assert.NoError(t, err)
		assert.True(t, connector != nil)

		assert.Nil(t, connector.Ping(context.Background()))
		assert.True(t, called)
	})

	t.Run("redisv8_with_on_connect_err", func(t *testing.T) {
		var called bool
		onConnect := func(ctx context.Context) error {
			called = true
			return nil
		}

		connector, err := NewConnector(model.RedisV8Type, WithRedisConfig(&model.RedisOptions{
			Addrs: []string{"localhost:8888"},
		}), model.WithOnConnect(onConnect))
		assert.NoError(t, err)
		assert.True(t, connector != nil)

		assert.NotNil(t, connector.Ping(context.Background()))
		assert.False(t, called)
	})
}
