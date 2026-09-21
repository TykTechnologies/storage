package redisv9

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/poolstats"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

func TestRedisV9_PoolStats(t *testing.T) {
	driver, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host:      "localhost",
		Port:      6379,
		MaxActive: 42,
	}))
	require.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, driver.Disconnect(context.Background())) })

	got, err := driver.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EngineRedis, got.Engine)
	assert.Equal(t, 42, got.MaxOpen)
	assert.Equal(t, 0, got.Open) // nothing dialed yet

	all := poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
		poolstats.FieldIdle | poolstats.FieldWaitCount | poolstats.FieldWaitDuration |
		poolstats.FieldCheckOutFailures
	assert.Equal(t, all, got.Present)
}

func TestRedisV9_PoolStats_ClusterOmitsMaxOpenAndWaits(t *testing.T) {
	// PoolSize is configured per cluster node while go-redis aggregates
	// Open/InUse/Idle across all nodes, so reporting a per-node MaxOpen next to
	// cluster-wide gauges would push utilization ratios past 100%. The wait
	// counters are omitted too: go-redis never aggregates WaitCount or
	// WaitDuration across cluster nodes, so they would read as a false zero.
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"localhost:7100"}})

	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	h := &RedisV9{
		client: client,
		cfg:    &model.RedisOptions{MaxActive: 9},
	}

	got, err := h.PoolStats(context.Background())
	require.NoError(t, err)

	assert.False(t, got.Present.Has(poolstats.FieldMaxOpen))
	assert.False(t, got.Present.Has(poolstats.FieldWaitCount))
	assert.False(t, got.Present.Has(poolstats.FieldWaitDuration))
	assert.Equal(t, 0, got.MaxOpen)
	assert.True(t, got.Present.Has(poolstats.FieldOpen))
	assert.True(t, got.Present.Has(poolstats.FieldCheckOutFailures), "timeouts are aggregated, so they stay present")
}

func TestRedisV9_PoolStats_ConnectorInheritsMaxOpen(t *testing.T) {
	conn, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host:      "localhost",
		Port:      6379,
		MaxActive: 7,
	}))
	require.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, conn.Disconnect(context.Background())) })

	kv, err := NewRedisV9WithConnection(conn)
	require.NoError(t, err)

	// MaxOpen is read from the shared client's own options, so it holds no
	// matter how the handler was constructed.
	got, err := kv.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 7, got.MaxOpen)
	assert.True(t, got.Present.Has(poolstats.FieldMaxOpen))
}

func TestRedisV9_PoolStats_ExternalClientReportsMaxOpen(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	h := &RedisV9{client: client}

	got, err := h.PoolStats(context.Background())
	require.NoError(t, err)

	// MaxOpen comes from the live client's options, so even an externally
	// supplied client reports the pool size it actually runs with (go-redis
	// fills in its own default when unset).
	assert.True(t, got.Present.Has(poolstats.FieldMaxOpen))
	assert.Equal(t, client.Options().PoolSize, got.MaxOpen)
	assert.Positive(t, got.MaxOpen)
	assert.True(t, got.Present.Has(poolstats.FieldWaitCount))
}

func TestStatsFromRedis_IdleNeverExceedsOpen(t *testing.T) {
	// go-redis reads TotalConns and IdleConns under separate lock acquisitions,
	// so a concurrent conn removal can yield idle > total. The mapping must
	// still uphold the documented Open = InUse + Idle invariant.
	got := statsFromRedis(&redis.PoolStats{TotalConns: 5, IdleConns: 8})

	assert.Equal(t, 5, got.Open)
	assert.Equal(t, 0, got.InUse)
	assert.Equal(t, 5, got.Idle)
	assert.Equal(t, got.Open, got.InUse+got.Idle)
}

// wrappingConnector simulates a consumer-defined model.Connector that wraps a
// *RedisV9 and only surfaces the client through As() — it is deliberately not
// a *RedisV9 itself.
type wrappingConnector struct {
	inner model.Connector
}

func (w *wrappingConnector) Disconnect(ctx context.Context) error { return w.inner.Disconnect(ctx) }
func (w *wrappingConnector) Ping(ctx context.Context) error       { return w.inner.Ping(ctx) }
func (w *wrappingConnector) Type() string                         { return w.inner.Type() }
func (w *wrappingConnector) As(i interface{}) bool                { return w.inner.As(i) }

func TestRedisV9_PoolStats_AfterWrappedConnectorDisconnect(t *testing.T) {
	inner, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host: "localhost",
		Port: 6379,
	}))
	require.NoError(t, err)

	kv, err := NewRedisV9WithConnection(&wrappingConnector{inner: inner})
	require.NoError(t, err)

	require.NoError(t, inner.Disconnect(context.Background()))

	// The closed flag is keyed by the shared client, so the disconnect must be
	// observable even though the handler was built through a wrapper that is
	// not a *RedisV9.
	_, err = kv.PoolStats(context.Background())
	assert.ErrorIs(t, err, temperr.ClosedConnection)
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}

func TestRedisV9_PoolStats_NoClient(t *testing.T) {
	h := &RedisV9{}

	_, err := h.PoolStats(context.Background())
	assert.ErrorIs(t, err, temperr.ClosedConnection)
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}

func TestRedisV9_PoolStats_AfterDisconnect(t *testing.T) {
	driver, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host: "localhost",
		Port: 6379,
	}))
	require.NoError(t, err)

	require.NoError(t, driver.Disconnect(context.Background()))

	// A disconnected handler must error like Ping does, not keep reporting
	// counters go-redis still returns for a closed pool.
	_, err = driver.PoolStats(context.Background())
	assert.ErrorIs(t, err, temperr.ClosedConnection)
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}

func TestRedisV9_PoolStats_AfterConnectorDisconnect(t *testing.T) {
	conn, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host: "localhost",
		Port: 6379,
	}))
	require.NoError(t, err)

	kv, err := NewRedisV9WithConnection(conn)
	require.NoError(t, err)

	require.NoError(t, conn.Disconnect(context.Background()))

	// The handler shares the connector's client (and closed flag), so closing
	// the connector must be observable through the handler too.
	_, err = kv.PoolStats(context.Background())
	assert.ErrorIs(t, err, temperr.ClosedConnection)
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}
