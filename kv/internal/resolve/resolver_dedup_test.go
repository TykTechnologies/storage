package resolve_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
	"github.com/stretchr/testify/require"
)

type countingProvider struct {
	value string
	mu    sync.Mutex
	calls map[string]int
	total atomic.Int32
}

func newCountingProvider(value string) *countingProvider {
	return &countingProvider{value: value, calls: map[string]int{}}
}

func (c *countingProvider) Get(_ context.Context, key string) (string, error) {
	c.total.Add(1)

	c.mu.Lock()
	c.calls[key]++
	c.mu.Unlock()

	return c.value, nil
}

func (c *countingProvider) callsFor(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls[key]
}

func buildDoc(ref string, count int) []byte {
	fields := make([]string, count)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"h%d":%q`, i, ref)
	}

	return []byte("{" + strings.Join(fields, ",") + "}")
}

func TestResolveAll_DeduplicatesIdenticalReferences(t *testing.T) {
	const n = 100

	provider := newCountingProvider("s3cr3t")
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	// Whole-value ref with no #fragment, so the raw value is returned as-is
	// (no JSON extraction needed for the mock value).
	doc := buildDoc("kv://vault/secret/data/app", n)

	_, err := r.ResolveAll(t.Context(), doc)
	require.NoError(t, err)

	require.Equal(t, 1, provider.callsFor("secret/data/app"),
		"identical references in one document must hit the backend once, not per-occurrence")
	require.EqualValues(t, 1, provider.total.Load(),
		"total backend calls must equal the number of UNIQUE references (1), not occurrences (%d)", n)
}

func TestResolveAll_DistinctReferencesEachResolvedOnce(t *testing.T) {
	const repeatEach = 10

	provider := newCountingProvider("v")
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	paths := []string{"secret/a", "secret/b", "secret/c"}

	var fields []string
	for i := 0; i < repeatEach; i++ {
		for j, p := range paths {
			fields = append(fields, fmt.Sprintf(`"h%d_%d":"kv://vault/%s"`, i, j, p))
		}
	}
	doc := []byte("{" + strings.Join(fields, ",") + "}")

	_, err := r.ResolveAll(t.Context(), doc)
	require.NoError(t, err)

	for _, p := range paths {
		require.Equal(t, 1, provider.callsFor(p),
			"each distinct reference must resolve exactly once (path %q)", p)
	}
	require.EqualValues(t, len(paths), provider.total.Load(),
		"total backend calls must equal the number of unique references")
}

func TestResolveAll_DedupsAcrossSyntaxForms(t *testing.T) {
	provider := newCountingProvider("s3cr3t")
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	doc := []byte(`{
		"whole": "kv://vault/secret/data/app",
		"inline": "prefix-$kv{vault:secret/data/app}-suffix"
	}`)

	_, err := r.ResolveAll(context.Background(), doc)
	require.NoError(t, err)

	require.EqualValues(t, int32(1), provider.total.Load(),
		"the same target via different syntax forms must resolve the backend once")
}

// mutableProvider returns whatever value it currently holds, and counts calls.
// It lets a test change the backing secret between resolutions.
type mutableProvider struct {
	mu    sync.Mutex
	value string
	calls int
}

func (m *mutableProvider) Get(_ context.Context, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++

	return m.value, nil
}

func (m *mutableProvider) set(v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = v
}

func TestResolveAll_MemoIsPerCallNotPersistent(t *testing.T) {
	provider := &mutableProvider{value: "old"}
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	doc := []byte(`{"a":"kv://vault/secret/data/app","b":"kv://vault/secret/data/app"}`)

	first, err := r.ResolveAll(t.Context(), doc)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"old","b":"old"}`, string(first))
	require.Equal(t, 1, provider.calls, "duplicate refs collapse to one backend call within a call")

	// Secret rotates between reloads.
	provider.set("new")

	second, err := r.ResolveAll(t.Context(), doc)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"new","b":"new"}`, string(second),
		"a later ResolveAll must see the rotated value, not a memo from the previous call")
	require.Equal(t, 2, provider.calls,
		"the second call must re-hit the backend — the memo must not persist across calls")
}
