package redisv8

import "context"

func (r *RedisV8) FlushAll(ctx context.Context) error {
	return r.client.FlushAll(ctx).Err()
}
