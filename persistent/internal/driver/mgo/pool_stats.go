package mgo

import (
	"context"
	"fmt"

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
func (d *mgoDriver) PoolStats(_ context.Context) (stats poolstats.PoolStats, err error) {
	if d.lifeCycle == nil || d.session == nil {
		return poolstats.PoolStats{}, fmt.Errorf("%s: %w", types.ErrorSessionClosed, poolstats.ErrClosed)
	}

	// mgo's stats switch is process-global: any code in the host process can
	// call mgo.SetStats(false) between our SetStats(true) and GetStats, and
	// GetStats dereferences the nil stats struct. Recover so a metrics poller
	// gets an error, not a panic.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mgo stats collection was disabled concurrently: %v", r)
		}
	}()

	// Idempotent: enables collection if off (keeps counters when already on)
	// and guards against mgo.GetStats' nil-stats panic.
	mgo.SetStats(true)

	return statsToPool(mgo.GetStats()), nil
}
