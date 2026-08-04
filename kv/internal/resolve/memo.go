package resolve

import (
	"context"
	"sync"
)

// rawResult memoizes a single backend fetch: the raw value or the error it
// failed with, for a {store, path} target.
type rawResult struct {
	raw string
	err error
}

// memoResult memoizes a fully resolved reference — the value after fragment
// extraction, or the error extraction (or the underlying fetch) produced.
type memoResult struct {
	val string
	err error
}

// memo is the per-ResolveAll-call resolution cache. It memoizes at two levels:
//
//   - raw dedups the backend fetch by {store, path}, so every fragment of one
//     secret triggers exactly one Get on the store.
//   - extract dedups fragment extraction by {store, path, fragment}, so an
//     identical reference repeated across the document is parsed only once.
//
// Both maps are guarded by a single mutex. The fetch level is populated
// concurrently during prefetch; the extract level only during the sequential
// substitution walk.
type memo struct {
	mu      sync.Mutex
	raw     map[pathKey]rawResult
	extract map[refKey]memoResult
}

func newMemo() *memo {
	return &memo{
		raw:     make(map[pathKey]rawResult),
		extract: make(map[refKey]memoResult),
	}
}

func (mm *memo) getRaw(k pathKey) (rawResult, bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	rr, ok := mm.raw[k]

	return rr, ok
}

func (mm *memo) setRaw(k pathKey, v rawResult) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.raw[k] = v
}

func (mm *memo) getExtract(k refKey) (memoResult, bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mr, ok := mm.extract[k]

	return mr, ok
}

func (mm *memo) setExtract(k refKey, v memoResult) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.extract[k] = v
}

type memoCtxKey struct{}

// withMemo attaches a fresh, per-call resolution memo to ctx.
func withMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, memoCtxKey{}, newMemo())
}

// memoFrom returns the per-call memo, or nil when ctx carries none (e.g. a
// direct Resolve call outside ResolveAll), in which case fetches are not
// memoized.
func memoFrom(ctx context.Context) *memo {
	//nolint:errcheck
	m, _ := ctx.Value(memoCtxKey{}).(*memo)
	return m
}
