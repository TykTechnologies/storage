package connector

import (
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
			name: "redisv8",
			typ:  model.RedisV8Type,
			opts: []model.Option{WithRedisConfig(&model.RedisOptions{
				Addrs: []string{"localhost:6379"},
			})},
			expectedErr: nil,
		},
		{
			name:        "redisv8",
			typ:         model.RedisV8Type,
			opts:        []model.Option{model.WithNoopConfig()},
			expectedErr: temperr.InvalidOptionsType,
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
