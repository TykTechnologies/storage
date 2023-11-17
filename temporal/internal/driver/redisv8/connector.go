package redisv8

import (
	"context"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/go-redis/redis/v8"
)

func (h *RedisV8) Disconnect(ctx context.Context) error {
	return h.client.Close()
}

func (h *RedisV8) Ping(ctx context.Context) error {
	return h.client.Ping(ctx).Err()
}

func (h *RedisV8) Type() string {
	return model.RedisV8Type
}

// As converts i to driver-specific types.
// redisv8 connector supports only *redis.UniversalClient.
// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
func (h *RedisV8) As(i interface{}) bool {
	if x, ok := i.(*redis.UniversalClient); ok {
		*x = h.client
		return true
	}

	return false
}
