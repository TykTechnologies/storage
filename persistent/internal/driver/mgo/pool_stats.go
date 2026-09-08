package mgo

import (
	"context"
	"errors"

	"gopkg.in/mgo.v2"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/poolstats"
)

var _ poolstats.PoolStatsProvider = (*mgoDriver)(nil)

// statsToPool maps mgo's socket counters onto the engine-agnostic PoolStats.
// mgo exposes neither a configured pool limit nor wait stats, so only
// Open/InUse/Idle are marked Present. All three are clamped at zero: a
// SetStats(false)/SetStats(true) cycle zeroes mgo's counters while sockets are
// still alive, after which their close events can drive the raw values negative.
func statsToPool(s mgo.Stats) poolstats.PoolStats {
	open := max(s.SocketsAlive, 0)
	inUse := max(s.SocketsInUse, 0)
	idle := max(open-inUse, 0)

	return poolstats.PoolStats{
		Engine:  poolstats.EngineMgo,
		Open:    open,
		InUse:   inUse,
		Idle:    idle,
		Present: poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle,
	}
}

// PoolStats implements poolstats.PoolStatsProvider. mgo only offers
// process-global counters (mgo.GetStats), so the numbers cover every mgo
// session in the process — best effort by design.
func (d *mgoDriver) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if d.lifeCycle == nil || d.session == nil {
		return poolstats.PoolStats{}, errors.New(types.ErrorSessionClosed)
	}

	// Idempotent: enables collection if off (keeps counters when already on)
	// and guards against mgo.GetStats' nil-stats panic.
	mgo.SetStats(true)

	return statsToPool(mgo.GetStats()), nil
}
