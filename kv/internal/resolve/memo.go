package resolve

import (
	"context"
	"sync"
)

// refKey identifies a resolution target.
type refKey struct {
	store    string
	path     string
	fragment string
}

// memoResult is a memoized resolution outcome (value or error).
type memoResult struct {
	val string
	err error
}

// memo is the per-ResolveAll-call resolution cache.
type memo struct {
	m  map[refKey]memoResult
	mu sync.Mutex
}

func (mm *memo) get(k refKey) (memoResult, bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mr, ok := mm.m[k]

	return mr, ok
}

func (mm *memo) set(k refKey, v memoResult) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.m[k] = v
}

type memoCtxKey struct{}

// withMemo attaches a fresh, per-call resolution memo to ctx.
func withMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, memoCtxKey{}, &memo{m: make(map[refKey]memoResult)})
}

// memoFrom returns the per-call memo, or nil when ctx carries none (e.g. a
// direct Resolve call outside ResolveAll), in which case fetches are not
// memoized.
func memoFrom(ctx context.Context) *memo {
	m, _ := ctx.Value(memoCtxKey{}).(*memo)
	return m
}
