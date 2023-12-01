package connector

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/temporal/model"
)

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
			connector, err := NewConnector(tt.typ, tt.opts...)
			assert.Equal(t, tt.expectedErr, err)

			if tt.expectedErr == nil {
				assert.True(t, connector != nil)
			}
		})
	}
}

func TestNewConnector_WithOnConnect(t *testing.T) {
	var called bool
	onConnect := func(ctx context.Context) error {
		called = true
		return nil
	}

	connector, err := NewConnector(model.RedisV8Type, WithRedisConfig(&model.RedisOptions{
		Addrs: []string{"localhost:6379"},
	}), model.WithOnConnect(onConnect))
	assert.NoError(t, err)
	assert.True(t, connector != nil)

	assert.Nil(t, connector.Ping(context.Background()))
	assert.True(t, called)
}
