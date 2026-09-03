package redisv9

import (
	"context"
	"time"

	"github.com/TykTechnologies/storage/poolstats"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

// defaultPoolSize is the pool size applied (per cluster node) when MaxActive
// is not configured. It must stay in sync with buildUniversalOptions.
const defaultPoolSize = 500

var _ poolstats.PoolStatsProvider = (*RedisV9)(nil)

// effectivePoolSize returns the pool size buildUniversalOptions configures on
// the client: MaxActive when positive, otherwise the default.
func effectivePoolSize(opts *model.RedisOptions) int {
	if opts.MaxActive > 0 {
		return opts.MaxActive
	}

	return defaultPoolSize
}

// poolCfg returns the RedisOptions this handler was built from. Handlers built
// with NewRedisV9WithConnection (e.g. via temporal.NewKeyValue) carry no config
// themselves, so fall back to the underlying connector's config.
func (h *RedisV9) poolCfg() *model.RedisOptions {
	if h.cfg != nil {
		return h.cfg
	}

	if parent, ok := h.connector.(*RedisV9); ok {
		return parent.cfg
	}

	return nil
}

// PoolStats implements poolstats.PoolStatsProvider. It reads go-redis' cheap
// in-process pool counters (aggregated across nodes in cluster mode) and never
// touches Redis. MaxOpen is only reported when the originating configuration
// is known.
func (h *RedisV9) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if h.client == nil {
		return poolstats.PoolStats{}, temperr.ClosedConnection
	}

	s := h.client.PoolStats()

	stats := poolstats.PoolStats{
		Engine:       poolstats.EngineRedis,
		Open:         int(s.TotalConns),
		InUse:        int(s.TotalConns) - int(s.IdleConns),
		Idle:         int(s.IdleConns),
		WaitCount:    int64(s.WaitCount),
		WaitDuration: time.Duration(s.WaitDurationNs),
		Present: poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle |
			poolstats.FieldWaitCount | poolstats.FieldWaitDuration,
	}

	if cfg := h.poolCfg(); cfg != nil {
		stats.MaxOpen = effectivePoolSize(cfg)
		stats.Present |= poolstats.FieldMaxOpen
	}

	return stats, nil
}
