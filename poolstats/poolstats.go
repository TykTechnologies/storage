// Package poolstats exposes engine-agnostic connection-pool statistics for the
// storage drivers in this module.
//
// Pool stats are opt-in: the large PersistentStorage/KeyValue interfaces are
// unchanged, and drivers additionally implement PoolStatsProvider. Consumers
// type-assert the value returned by the existing factories:
//
//	store, _ := persistent.NewPersistentStorage(opts)
//	if p, ok := store.(poolstats.PoolStatsProvider); ok {
//		stats, err := p.PoolStats(ctx)
//		if err == nil && stats.Present.Has(poolstats.FieldInUse) {
//			metrics.Gauge("pool_in_use", stats.InUse)
//		}
//	}
//
// Not every backend reports every field. Only emit a metric for a field whose
// bit is set in Present; a zero value with the bit unset means "not reported",
// not "zero".
package poolstats

import (
	"context"
	"time"
)

// Engine values reported in PoolStats.Engine.
const (
	EnginePostgres = "postgres"
	EngineMongo    = "mongo"
	EngineMgo      = "mgo"
	EngineRedis    = "redis"
)

// FieldSet is a bitmask describing which PoolStats fields a backend reports.
type FieldSet uint8

// Field bits for FieldSet, one per reportable PoolStats field.
const (
	FieldMaxOpen FieldSet = 1 << iota
	FieldOpen
	FieldInUse
	FieldIdle
	FieldWaitCount
	FieldWaitDuration
)

// Has reports whether every bit in field is set in f.
func (f FieldSet) Has(field FieldSet) bool {
	return f&field == field && field != 0
}

// PoolStats is an engine-agnostic snapshot of a driver's connection pool.
type PoolStats struct {
	// Engine identifies the backend: "postgres", "mongo", "mgo" or "redis".
	Engine string
	// MaxOpen is the configured maximum pool size; 0 means unlimited/unset.
	MaxOpen int
	// Open is the number of established connections (in use + idle).
	Open int
	// InUse is the number of connections currently checked out.
	InUse int
	// Idle is the number of idle connections.
	Idle int
	// WaitCount is the cumulative number of waits for a connection.
	WaitCount int64
	// WaitDuration is the cumulative time blocked waiting for a connection.
	WaitDuration time.Duration
	// Present marks which of the above fields this backend actually reports.
	Present FieldSet
}

// PoolStatsProvider is an optional capability interface implemented by drivers
// that can report pool statistics. Reading stats is cheap and never opens a
// connection or probes the backing dependency.
type PoolStatsProvider interface {
	PoolStats(ctx context.Context) (PoolStats, error)
}
