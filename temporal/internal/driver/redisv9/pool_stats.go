package redisv9

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/TykTechnologies/storage/poolstats"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

var _ poolstats.PoolStatsProvider = (*RedisV9)(nil)

// PoolStats implements poolstats.PoolStatsProvider. It reads go-redis' cheap
// in-process pool counters (aggregated across nodes in cluster mode) and never
// touches Redis. MaxOpen is read from the live client's own options — the
// source of truth for the pool actually serving requests, regardless of how
// this handler was constructed. Cluster clients are excluded: their pool size
// applies per node while Open/InUse/Idle are cluster-wide aggregates, so
// reporting it would make utilization ratios exceed 100%.
func (h *RedisV9) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if h.client == nil || (h.closed != nil && h.closed.Load()) {
		return poolstats.PoolStats{}, temperr.ClosedConnection
	}

	s := h.client.PoolStats()

	stats := poolstats.PoolStats{
		Engine:           poolstats.EngineRedis,
		Open:             int(s.TotalConns),
		InUse:            int(s.TotalConns) - int(s.IdleConns),
		Idle:             int(s.IdleConns),
		WaitCount:        int64(s.WaitCount),
		WaitDuration:     time.Duration(s.WaitDurationNs),
		CheckOutFailures: int64(s.Timeouts),
		Present: poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle |
			poolstats.FieldWaitCount | poolstats.FieldWaitDuration | poolstats.FieldCheckOutFailures,
	}

	// Both the simple and failover (sentinel) clients are *redis.Client; the
	// cluster client is not, so it is skipped without a separate check.
	if c, ok := h.client.(*redis.Client); ok {
		stats.MaxOpen = c.Options().PoolSize
		stats.Present |= poolstats.FieldMaxOpen
	}

	return stats, nil
}
