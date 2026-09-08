package mongo

import (
	"context"
	"errors"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/event"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/poolstats"
)

// defaultMaxPoolSize is the pool size this driver applies when maxPoolSize is
// not set via URI or options. It matches the mongo-go driver's own default
// (100), but Connect pins it explicitly on the client options, so the limit
// the pool enforces and the MaxOpen reported by PoolStats are the same value
// by construction — a driver upgrade cannot silently desync them.
const defaultMaxPoolSize = 100

var _ poolstats.PoolStatsProvider = (*mongoDriver)(nil)

// poolStatsCollector aggregates event.PoolMonitor callbacks into atomic
// counters. The official driver has no snapshot pool API, so this is wired at
// client construction time and read lock-free afterwards.
type poolStatsCollector struct {
	maxPoolSize uint64

	created        atomic.Int64
	closed         atomic.Int64
	checkedOut     atomic.Int64
	checkedIn      atomic.Int64
	checkOutFailed atomic.Int64
}

// newPoolStatsCollector takes the already-resolved pool size: Connect pins
// MaxPoolSize on the client options before building the collector, so there is
// exactly one owner of the default and the two values cannot diverge.
func newPoolStatsCollector(maxPoolSize uint64) *poolStatsCollector {
	return &poolStatsCollector{maxPoolSize: maxPoolSize}
}

func (c *poolStatsCollector) monitor() *event.PoolMonitor {
	return &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			switch e.Type {
			case event.ConnectionCreated:
				c.created.Add(1)
			case event.ConnectionClosed:
				c.closed.Add(1)
			case event.GetSucceeded:
				c.checkedOut.Add(1)
			case event.ConnectionReturned:
				c.checkedIn.Add(1)
			case event.GetFailed:
				c.checkOutFailed.Add(1)
			}
		},
	}
}

func (c *poolStatsCollector) snapshot() poolstats.PoolStats {
	open := max(c.created.Load()-c.closed.Load(), 0)
	inUse := max(c.checkedOut.Load()-c.checkedIn.Load(), 0)
	idle := max(open-inUse, 0)

	return poolstats.PoolStats{
		Engine:           poolstats.EngineMongo,
		MaxOpen:          int(c.maxPoolSize),
		Open:             int(open),
		InUse:            int(inUse),
		Idle:             int(idle),
		CheckOutFailures: c.checkOutFailed.Load(),
		Present: poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
			poolstats.FieldIdle | poolstats.FieldCheckOutFailures,
	}
}

// PoolStats implements poolstats.PoolStatsProvider. Stats come from the
// event.PoolMonitor wired in Connect; reading them never touches MongoDB. The
// collector pointer is loaded atomically because Connect swaps it and Close
// clears it while metrics pollers read it concurrently; nil means the session
// is closed (or never connected) and must error, not report healthy zeros.
func (d *mongoDriver) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if d.lifeCycle == nil {
		return poolstats.PoolStats{}, errors.New(types.ErrorSessionClosed)
	}

	collector := d.poolStats.Load()
	if collector == nil {
		return poolstats.PoolStats{}, errors.New(types.ErrorSessionClosed)
	}

	return collector.snapshot(), nil
}
