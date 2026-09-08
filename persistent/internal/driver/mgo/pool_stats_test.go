//go:build mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mgo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2"

	"github.com/TykTechnologies/storage/poolstats"
)

func TestStatsToPool(t *testing.T) {
	got := statsToPool(mgo.Stats{SocketsAlive: 5, SocketsInUse: 2})

	assert.Equal(t, poolstats.EngineMgo, got.Engine)
	assert.Equal(t, 5, got.Open)
	assert.Equal(t, 2, got.InUse)
	assert.Equal(t, 3, got.Idle)

	want := poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle
	assert.Equal(t, want, got.Present)
	assert.False(t, got.Present.Has(poolstats.FieldMaxOpen), "mgo does not report a pool limit")
	assert.False(t, got.Present.Has(poolstats.FieldWaitCount))
	assert.False(t, got.Present.Has(poolstats.FieldWaitDuration))
	assert.Equal(t, 0, got.MaxOpen)
}

func TestStatsToPool_ClampsNegativeCounters(t *testing.T) {
	// A SetStats(false)/SetStats(true) cycle zeroes mgo's global counters while
	// sockets are still alive; their close events then drive the raw values
	// negative. Reported stats must never go below zero.
	got := statsToPool(mgo.Stats{SocketsAlive: -3, SocketsInUse: -1})

	assert.Equal(t, 0, got.Open)
	assert.Equal(t, 0, got.InUse)
	assert.Equal(t, 0, got.Idle)
}

func TestMgoDriver_PoolStats_NotConnected(t *testing.T) {
	d := &mgoDriver{lifeCycle: &lifeCycle{}}

	_, err := d.PoolStats(context.Background())
	assert.Error(t, err)
}

func TestMgoDriver_PoolStats_Integration(t *testing.T) {
	d, _ := prepareEnvironment(t)

	got, err := d.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EngineMgo, got.Engine)
	assert.True(t, got.Present.Has(poolstats.FieldOpen))
	assert.GreaterOrEqual(t, got.Open, got.InUse)
}
