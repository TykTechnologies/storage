package postgres

import (
	"context"
	"database/sql"

	"github.com/TykTechnologies/storage/poolstats"
)

var _ poolstats.PoolStatsProvider = (*driver)(nil)

// poolStatsFromSQL maps database/sql pool stats onto the engine-agnostic
// poolstats.PoolStats. sql.DB reports every field, so all Present bits are set.
func poolStatsFromSQL(s sql.DBStats) poolstats.PoolStats {
	return poolstats.PoolStats{
		Engine:       poolstats.EnginePostgres,
		MaxOpen:      s.MaxOpenConnections,
		Open:         s.OpenConnections,
		InUse:        s.InUse,
		Idle:         s.Idle,
		WaitCount:    s.WaitCount,
		WaitDuration: s.WaitDuration,
		Present: poolstats.FieldMaxOpen | poolstats.FieldOpen | poolstats.FieldInUse |
			poolstats.FieldIdle | poolstats.FieldWaitCount | poolstats.FieldWaitDuration,
	}
}

// PoolStats implements poolstats.PoolStatsProvider. It reads the atomic
// snapshot kept by database/sql and never touches the database. The d.db check
// matches Ping's closed-session semantics: Close() nils d.db but leaves d.sqlDB
// set, and a closed pool must error rather than report healthy zeros.
func (d *driver) PoolStats(_ context.Context) (poolstats.PoolStats, error) {
	if d.lifeCycle == nil || d.db == nil || d.sqlDB == nil {
		return poolstats.PoolStats{}, ErrorSessionClosed
	}

	return poolStatsFromSQL(d.sqlDB.Stats()), nil
}
