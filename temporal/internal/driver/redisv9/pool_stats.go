package redisv9

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/TykTechnologies/storage/poolstats"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

var _ poolstats.PoolStatsProvider = (*RedisV9)(nil)

// PoolStats implements poolstats.PoolStatsProvider. It reads go-redis' cheap
// in-process pool counters (aggregated across nodes in cluster mode). It does
// not issue commands, with one narrow exception: a cluster client whose
// cluster state was never loaded (no command has succeeded yet) fetches it
// synchronously, so the first read on an idle cluster client can touch Redis.
//
// MaxOpen is read from the live client's own options — the source of truth for
// the pool actually serving requests, regardless of how this handler was
// constructed. Cluster clients report neither MaxOpen (the pool size applies
// per node while Open/InUse/Idle are cluster-wide aggregates, so utilization
// ratios would exceed 100%) nor the wait fields (go-redis does not aggregate
// WaitCount/WaitDuration across nodes, so they would read as a false zero).
func (h *RedisV9) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if h.client == nil || h.isClosed() {
		return poolstats.PoolStats{}, fmt.Errorf("%w: %w", temperr.ClosedConnection, poolstats.ErrClosed)
	}

	s := h.client.PoolStats()
	stats := statsFromRedis(s)

	// Both the simple and failover (sentinel) clients are *redis.Client; the
	// cluster client is not, so it is skipped without a separate check.
	if c, ok := h.client.(*redis.Client); ok {
		stats.MaxOpen = c.Options().PoolSize
		stats.WaitCount = int64(s.WaitCount)
		stats.WaitDuration = time.Duration(s.WaitDurationNs)
		stats.Present |= poolstats.FieldMaxOpen | poolstats.FieldWaitCount | poolstats.FieldWaitDuration
	}

	return stats, nil
}

// statsFromRedis maps go-redis pool counters onto the engine-agnostic fields
// every client kind reports. TotalConns and IdleConns are read under separate
// lock acquisitions inside go-redis, so idle can transiently exceed total;
// InUse is clamped and Idle derived from the clamped pair so the documented
// Open = InUse + Idle invariant holds on every snapshot.
func statsFromRedis(s *redis.PoolStats) poolstats.PoolStats {
	open := int(s.TotalConns)
	inUse := max(open-int(s.IdleConns), 0)

	return poolstats.PoolStats{
		Engine:           poolstats.EngineRedis,
		Open:             open,
		InUse:            inUse,
		Idle:             open - inUse,
		CheckOutFailures: int64(s.Timeouts),
		Present: poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle |
			poolstats.FieldCheckOutFailures,
	}
}
