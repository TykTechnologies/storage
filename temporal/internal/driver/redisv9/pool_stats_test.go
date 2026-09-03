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

func TestEffectivePoolSize(t *testing.T) {
	assert.Equal(t, 500, effectivePoolSize(&model.RedisOptions{}))
	assert.Equal(t, 42, effectivePoolSize(&model.RedisOptions{MaxActive: 42}))
}

func TestRedisV9_PoolStats(t *testing.T) {
	driver, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host:      "localhost",
		Port:      6379,
		MaxActive: 42,
	}))
	require.NoError(t, err)

	got, err := driver.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EngineRedis, got.Engine)
	assert.Equal(t, 42, got.MaxOpen)
	assert.Equal(t, 0, got.Open) // nothing dialed yet

	all := poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
		poolstats.FieldIdle | poolstats.FieldWaitCount | poolstats.FieldWaitDuration
	assert.Equal(t, all, got.Present)
}

func TestRedisV9_PoolStats_ConnectorInheritsMaxOpen(t *testing.T) {
	conn, err := NewRedisV9WithOpts(model.WithRedisConfig(&model.RedisOptions{
		Host:      "localhost",
		Port:      6379,
		MaxActive: 7,
	}))
	require.NoError(t, err)

	kv, err := NewRedisV9WithConnection(conn)
	require.NoError(t, err)

	got, err := kv.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 7, got.MaxOpen)
	assert.True(t, got.Present.Has(poolstats.FieldMaxOpen))
}

func TestRedisV9_PoolStats_UnknownConfigOmitsMaxOpen(t *testing.T) {
	h := &RedisV9{client: redis.NewClient(&redis.Options{Addr: "localhost:6379"})}

	got, err := h.PoolStats(context.Background())
	require.NoError(t, err)

	// The backend cannot report MaxOpen here: the bit must NOT be present and
	// the zero value must not be mistaken for "unlimited".
	assert.False(t, got.Present.Has(poolstats.FieldMaxOpen))
	assert.Equal(t, 0, got.MaxOpen)
	assert.True(t, got.Present.Has(poolstats.FieldWaitCount))
}

func TestRedisV9_PoolStats_NoClient(t *testing.T) {
	h := &RedisV9{}

	_, err := h.PoolStats(context.Background())
	assert.ErrorIs(t, err, temperr.ClosedConnection)
}
