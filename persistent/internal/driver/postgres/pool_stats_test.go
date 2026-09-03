//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/poolstats"
)

func TestPoolStatsFromSQL(t *testing.T) {
	in := sql.DBStats{
		MaxOpenConnections: 25,
		OpenConnections:    7,
		InUse:              3,
		Idle:               4,
		WaitCount:          9,
		WaitDuration:       1500 * time.Millisecond,
	}

	got := poolStatsFromSQL(in)

	assert.Equal(t, poolstats.EnginePostgres, got.Engine)
	assert.Equal(t, 25, got.MaxOpen)
	assert.Equal(t, 7, got.Open)
	assert.Equal(t, 3, got.InUse)
	assert.Equal(t, 4, got.Idle)
	assert.Equal(t, int64(9), got.WaitCount)
	assert.Equal(t, 1500*time.Millisecond, got.WaitDuration)

	all := poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
		poolstats.FieldIdle | poolstats.FieldWaitCount | poolstats.FieldWaitDuration
	assert.Equal(t, all, got.Present)
}

func TestDriver_PoolStats_SessionClosed(t *testing.T) {
	d := &driver{lifeCycle: &lifeCycle{}}

	_, err := d.PoolStats(context.Background())
	assert.ErrorIs(t, err, ErrorSessionClosed)
}

func TestDriver_PoolStats_Integration(t *testing.T) {
	d, err := NewPostgresDriver(&types.ClientOpts{ConnectionString: connStr, Type: "postgres"})
	require.NoError(t, err)

	defer func() { _ = d.Close() }()

	got, err := d.PoolStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, poolstats.EnginePostgres, got.Engine)
	assert.True(t, got.Present.Has(poolstats.FieldOpen))
	assert.True(t, got.Present.Has(poolstats.FieldWaitCount))
	assert.Equal(t, got.Open, got.InUse+got.Idle)
	assert.GreaterOrEqual(t, got.Open, 0)

	// Compile-time proof the concrete driver satisfies the capability interface
	// through the public factory's return type.
	var provider poolstats.PoolStatsProvider = d
	assert.NotNil(t, provider)
}
