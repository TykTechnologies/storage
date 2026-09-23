package poolstats_test

import (
	"context"
	"fmt"

	"github.com/TykTechnologies/storage/poolstats"
)

// Example shows the opt-in type-assertion pattern. `store` stands in for the
// value returned by persistent.NewPersistentStorage, connector.NewConnector or
// temporal.NewKeyValue.
func Example() {
	var store interface{}

	if p, ok := store.(poolstats.PoolStatsProvider); ok {
		stats, err := p.PoolStats(context.Background())
		if err == nil && stats.Present.Has(poolstats.FieldInUse) {
			fmt.Printf("%s: %d connections in use\n", stats.Engine, stats.InUse)
		}
	}
	// Output:
}
