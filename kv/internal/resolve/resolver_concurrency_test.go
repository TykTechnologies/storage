package resolve_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
	"github.com/stretchr/testify/require"
)

// barrierProvider blocks every Get until exactly n calls are simultaneously
// in-flight, then releases them all.
type barrierProvider struct {
	n     int
	value string

	mu      sync.Mutex
	arrived int
	peak    int

	gate chan struct{}
	once sync.Once
}

func newBarrierProvider(n int, value string) *barrierProvider {
	return &barrierProvider{n: n, value: value, gate: make(chan struct{})}
}

func (b *barrierProvider) Get(ctx context.Context, _ string) (string, error) {
	b.mu.Lock()
	b.arrived++
	if b.arrived > b.peak {
		b.peak = b.arrived
	}
	reached := b.arrived >= b.n
	b.mu.Unlock()

	if reached {
		b.once.Do(func() { close(b.gate) })
	}

	select {
	case <-b.gate:
		return b.value, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (b *barrierProvider) peakConcurrency() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.peak
}

func TestResolveAll_ResolvesDistinctReferencesConcurrently(t *testing.T) {
	const n = 8

	provider := newBarrierProvider(n, "v")
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	// n DISTINCT references so dedup does not collapse them — we want n
	// independent fetches that can only complete if run concurrently.
	fields := make([]string, n)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"h%d":"kv://vault/secret/%d"`, i, i)
	}
	doc := []byte("{" + strings.Join(fields, ",") + "}")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := r.ResolveAll(ctx, doc)
	require.NoError(t, err,
		"distinct references must resolve concurrently; serial resolution never reaches the barrier and times out")
	require.Equal(t, n, provider.peakConcurrency(),
		"all %d distinct fetches must be in flight simultaneously", n)
}
