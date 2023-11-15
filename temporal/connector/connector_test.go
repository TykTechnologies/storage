package connector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/temporal/connector/types"
)

func TestNewConnector(t *testing.T) {
	tests := []struct {
		name        string
		typ         string
		opts        []types.Option
		expectedErr error
	}{
		{
			name:        "default",
			typ:         "",
			opts:        []types.Option{},
			expectedErr: types.ErrInvalidHandlerType,
		},
		{
			name: "redisv8",
			typ:  types.RedisV8Type,
			opts: []types.Option{WithRedisConfig(&types.RedisOptions{
				Addrs: []string{"localhost:6379"},
			})},
			expectedErr: nil,
		},
		{
			name:        "redisv8",
			typ:         types.RedisV8Type,
			opts:        []types.Option{types.WithNoopConfig()},
			expectedErr: types.ErrInvalidOptionsType,
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
