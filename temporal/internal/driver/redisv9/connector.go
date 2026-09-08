package redisv9

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/TykTechnologies/storage/temporal/model"
)

func (h *RedisV9) Disconnect(ctx context.Context) error {
	// Mark closed before Close so PoolStats errors immediately: go-redis keeps
	// returning pool counters after Close, which would look like a healthy
	// empty pool to a metrics poller.
	if h.closed != nil {
		h.closed.Store(true)
	}

	return h.client.Close()
}

// isClosed reports whether Disconnect was called on this handler (or on the
// connector it shares its client with). The flag is nil only on hand-built
// struct literals, which count as open.
func (h *RedisV9) isClosed() bool {
	return h.closed != nil && h.closed.Load()
}

func (h *RedisV9) Ping(ctx context.Context) error {
	return h.client.Ping(ctx).Err()
}

func (h *RedisV9) Type() string {
	return model.RedisV9Type
}

// As converts i to driver-specific types.
// redisv9 connector supports only *redis.UniversalClient.
// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
func (h *RedisV9) As(i interface{}) bool {
	if x, ok := i.(*redis.UniversalClient); ok {
		*x = h.client
		return true
	}

	return false
}
