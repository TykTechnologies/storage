package redisv8

import (
	"context"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/go-redis/redis/v8"
)

func (r *RedisV8) FlushAll(ctx context.Context) error {
	switch client := r.client.(type) {
	case *redis.ClusterClient:
		return client.ForEachMaster(ctx, func(context context.Context, client *redis.Client) error {
			return client.FlushAll(ctx).Err()
		})
	case *redis.Client:
		return r.client.FlushAll(ctx).Err()
	default:
		return temperr.InvalidHandlerType
	}
}
