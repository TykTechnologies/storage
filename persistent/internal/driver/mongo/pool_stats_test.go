//go:build mongo7 || mongo6 || mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo7 mongo6 mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/TykTechnologies/storage/poolstats"
)

func TestPoolStatsCollector_Snapshot(t *testing.T) {
	c := newPoolStatsCollector(options.Client().SetMaxPoolSize(50))
	m := c.monitor()

	for i := 0; i < 4; i++ {
		m.Event(&event.PoolEvent{Type: event.ConnectionCreated})
	}

	m.Event(&event.PoolEvent{Type: event.ConnectionClosed})
	m.Event(&event.PoolEvent{Type: event.GetSucceeded})
	m.Event(&event.PoolEvent{Type: event.GetSucceeded})
	m.Event(&event.PoolEvent{Type: event.ConnectionReturned})
	m.Event(&event.PoolEvent{Type: event.GetFailed}) // must NOT count as checked out

	got := c.snapshot()

	assert.Equal(t, poolstats.EngineMongo, got.Engine)
	assert.Equal(t, 50, got.MaxOpen)
	assert.Equal(t, 3, got.Open)  // 4 created - 1 closed
	assert.Equal(t, 1, got.InUse) // 2 checked out - 1 checked in
	assert.Equal(t, 2, got.Idle)  // open - in use
	assert.Equal(t, int64(1), c.checkOutFailed.Load())

	want := poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle
	assert.Equal(t, want, got.Present)
	assert.False(t, got.Present.Has(poolstats.FieldWaitCount), "mongo does not report waits")
	assert.False(t, got.Present.Has(poolstats.FieldWaitDuration), "mongo does not report waits")
}

func TestPoolStatsCollector_DefaultMaxPoolSize(t *testing.T) {
	c := newPoolStatsCollector(options.Client())
	assert.Equal(t, 100, c.snapshot().MaxOpen)
}

func TestMongoDriver_PoolStats_NotConnected(t *testing.T) {
	d := &mongoDriver{lifeCycle: &lifeCycle{}}

	_, err := d.PoolStats(context.Background())
	assert.Error(t, err)
}

func TestMongoDriver_PoolStats_Integration(t *testing.T) {
	// This test writes nothing, so no DB cleanup is needed.
	d, _ := prepareEnvironment(t)

	got, err := d.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EngineMongo, got.Engine)
	assert.Equal(t, 100, got.MaxOpen) // driver default, no maxPoolSize in test URI
	assert.True(t, got.Present.Has(poolstats.FieldOpen))
	assert.GreaterOrEqual(t, got.Open, 0)
	assert.GreaterOrEqual(t, got.InUse, 0)
	assert.GreaterOrEqual(t, got.Idle, 0)
}
