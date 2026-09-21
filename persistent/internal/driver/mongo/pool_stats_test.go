//go:build mongo7 || mongo6 || mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo7 mongo6 mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/event"

	"github.com/TykTechnologies/storage/poolstats"
)

func TestPoolStatsCollector_Snapshot(t *testing.T) {
	c := newPoolStatsCollector(50)
	m := c.monitor()

	m.Event(&event.PoolEvent{Type: event.PoolCreated})

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
	assert.Equal(t, int64(1), got.CheckOutFailures)

	want := poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
		poolstats.FieldIdle | poolstats.FieldCheckOutFailures
	assert.Equal(t, want, got.Present)
	assert.False(t, got.Present.Has(poolstats.FieldWaitCount), "mongo does not report waits")
	assert.False(t, got.Present.Has(poolstats.FieldWaitDuration), "mongo does not report waits")
}

func TestPoolStatsCollector_MaxOpenScalesWithServerPools(t *testing.T) {
	// maxPoolSize applies per server while the connection counters aggregate
	// across all servers, so MaxOpen must scale with the live pool count or a
	// replica set would show utilization above 100%.
	c := newPoolStatsCollector(50)
	m := c.monitor()

	// No PoolCreated yet: clamp to one pool rather than report 0 ("unlimited").
	assert.Equal(t, 50, c.snapshot().MaxOpen)

	for i := 0; i < 3; i++ {
		m.Event(&event.PoolEvent{Type: event.PoolCreated})
	}

	assert.Equal(t, 150, c.snapshot().MaxOpen)

	m.Event(&event.PoolEvent{Type: event.PoolClosedEvent})

	assert.Equal(t, 100, c.snapshot().MaxOpen)
}

func TestMongoDriver_PoolStats_NotConnected(t *testing.T) {
	d := &mongoDriver{lifeCycle: &lifeCycle{}}

	_, err := d.PoolStats(context.Background())
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}

func TestMongoDriver_PoolStats_Integration(t *testing.T) {
	// This test writes nothing, so no DB cleanup is needed.
	d, _ := prepareEnvironment(t)

	got, err := d.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EngineMongo, got.Engine)
	assert.Equal(t, 100, got.MaxOpen) // default per-server size × 1 pool (single-node test mongo)
	assert.True(t, got.Present.Has(poolstats.FieldOpen))
	assert.GreaterOrEqual(t, got.Open, 0)
	assert.GreaterOrEqual(t, got.InUse, 0)
	assert.GreaterOrEqual(t, got.Idle, 0)
}

func TestMongoDriver_PoolStats_AfterClose_Integration(t *testing.T) {
	d, _ := prepareEnvironment(t)

	require.NoError(t, d.Close())

	// A closed store must error like the other drivers do, not keep reporting
	// healthy zeros that a metrics loop would emit forever.
	_, err := d.PoolStats(context.Background())
	assert.ErrorIs(t, err, poolstats.ErrClosed)
}
