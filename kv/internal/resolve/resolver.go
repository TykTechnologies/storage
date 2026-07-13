package resolve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/TykTechnologies/storage/kv"
)

// maxConcurrentResolves bounds how many references are fetched at once during
// the prefetch phase.
const maxConcurrentResolves = 16

type Resolver struct {
	registry kv.StoreGetter
	lenient  bool
}

type Option func(*Resolver)

// WithLenientMode makes the resolver leave any reference that targets an
// unknown store (kv.ErrStoreNotFound) unchanged, instead of failing.
//
// It exists for exactly one caller: Phase 1 of the registry bootstrap,
// which resolves store configs before the remote stores they may reference
// have been initialized. At that point the absent stores are precisely the
// remote ones, and their references must pass through verbatim so the
// caller's later strict pass can resolve them.
//
// Lenient mode tolerates absent stores only: malformed references,
// reachable stores with missing keys, and missing JSON fields still fail,
// so config typos are never silently masked. Deliberately not exposed
// through the public kv/resolver facade — exposing it would invite callers
// to suppress resolution errors wholesale.
func WithLenientMode() Option {
	return func(r *Resolver) {
		r.lenient = true
	}
}

func NewResolver(registry kv.StoreGetter, opts ...Option) *Resolver {
	r := &Resolver{registry: registry}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

var inlineRe = regexp.MustCompile(`\$kv\{([^}]+)\}`)

func (r *Resolver) Resolve(ctx context.Context, input string) (string, error) {
	ref, ok, err := parseWholeValue(input)
	if err != nil {
		return "", err
	}

	if ok {
		return r.resolveRef(ctx, ref, input)
	}

	return r.resolveInline(ctx, input)
}

// resolveRef resolves a single reference. literal is the original text to emit
// unchanged when lenient mode tolerates a missing store — the whole input for a
// kv:// reference, or the matched token for a $kv{} one. It is the single place
// the lenient store-not-found rule lives.
func (r *Resolver) resolveRef(ctx context.Context, ref refKey, literal string) (string, error) {
	val, err := r.fetchAndExtract(ctx, ref.store, ref.path, ref.fragment)
	if err != nil {
		if r.lenient && errors.Is(err, kv.ErrStoreNotFound) {
			return literal, nil
		}

		return "", err
	}

	return val, nil
}

// resolveInline replaces every $kv{...} token in input. A malformed or
// unresolvable token is left in place and its error collected; all failures are
// returned joined so the caller sees every problem at once.
func (r *Resolver) resolveInline(ctx context.Context, input string) (string, error) {
	// The token regex requires a closing brace, so an unclosed "$kv{" can never
	// match — without this check a typo'd reference would silently pass through
	// as a literal value.
	if unclosedInlineToken(input) >= 0 {
		return "", fmt.Errorf(
			"%w: unclosed $kv{ reference in %q",
			ErrMalformedReference,
			input,
		)
	}

	var errs []error

	result := inlineRe.ReplaceAllStringFunc(input, func(match string) string {
		ref, err := parseInlineToken(match)
		if err != nil {
			errs = append(errs, err)

			return match
		}

		val, err := r.resolveRef(ctx, ref, match)
		if err != nil {
			errs = append(errs, err)

			return match
		}

		return val
	})

	if len(errs) > 0 {
		return "", errors.Join(errs...)
	}

	return result, nil
}

func (r *Resolver) ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error) {
	// Fast path: skip unmarshal/remarshal entirely when no KV syntax is present,
	// preserving the original bytes and avoiding unnecessary allocations.
	if !bytes.Contains(rawJSON, []byte("kv://")) && !bytes.Contains(rawJSON, []byte("$kv{")) {
		// Without this check, JSON validation would depend on whether
		// the document happens to contain KV syntax.
		if !json.Valid(rawJSON) {
			return nil, fmt.Errorf("%w: invalid document", ErrInvalidJSON)
		}

		return rawJSON, nil
	}

	// UseNumber keeps numbers as json.Number instead of float64. A float64
	// round-trip silently corrupts integers above 2^53 (IDs, nanosecond
	// timestamps).
	dec := json.NewDecoder(bytes.NewReader(rawJSON))
	dec.UseNumber()

	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}

	ctx = withMemo(ctx)

	// Prefetch every distinct reference concurrently into the memo, so the
	// sequential substitution walk below reads them without further I/O.
	r.prefetch(ctx, doc)

	resolved, err := r.walkAndResolve(ctx, doc)
	if err != nil {
		return nil, err
	}

	// Encoder with SetEscapeHTML(false) keeps &, <, > literal
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(resolved); err != nil {
		return nil, err
	}

	// Encode appends a trailing newline; Marshal does not.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// prefetch resolves every distinct reference in doc concurrently, warming the
// per-call memo so the substitution walk reads them without further I/O.
//
// It is best-effort and deliberately non-cancelling. Each goroutine returns nil
// even on failure — to keep one reference's failure from poisoning the others.
func (r *Resolver) prefetch(ctx context.Context, doc any) {
	refs := collectRefs(doc)

	if len(refs) <= 1 {
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentResolves)

	for ref := range refs {
		g.Go(func() error {
			//nolint:errcheck
			_, _ = r.fetchAndExtract(gctx, ref.store, ref.path, ref.fragment)
			return nil
		})
	}

	//nolint:errcheck
	_ = g.Wait()
}

// fetchAndExtract resolves a single target, consulting the per-call memo (when
// ctx carries one) so repeated references — in either syntax form — resolve the
// backend at most once per document.
func (r *Resolver) fetchAndExtract(ctx context.Context, storeName, path, fragment string) (string, error) {
	mm := memoFrom(ctx)

	key := refKey{store: storeName, path: path, fragment: fragment}
	if mm != nil {
		if hit, ok := mm.get(key); ok {
			return hit.val, hit.err
		}
	}

	val, err := r.fetch(ctx, storeName, path, fragment)

	if mm != nil {
		mm.set(key, memoResult{val: val, err: err})
	}

	return val, err
}

func (r *Resolver) fetch(ctx context.Context, storeName, path, fragment string) (string, error) {
	store, err := r.registry.GetStore(storeName)
	if err != nil {
		return "", err
	}

	raw, err := store.Get(ctx, path)
	if err != nil {
		return "", err
	}

	if fragment == "" {
		return raw, nil
	}

	return extractJSONPointer(raw, fragment)
}

// unclosedInlineToken returns the index of the first "$kv{" occurrence in
// input that is not the start of a well-formed $kv{...} token, or -1 when
// every occurrence is properly closed.
func unclosedInlineToken(input string) int {
	starts := make(map[int]struct{})
	for _, m := range inlineRe.FindAllStringIndex(input, -1) {
		starts[m[0]] = struct{}{}
	}

	offset := 0

	for {
		i := strings.Index(input[offset:], "$kv{")
		if i < 0 {
			return -1
		}

		abs := offset + i
		if _, ok := starts[abs]; !ok {
			return abs
		}

		offset = abs + len("$kv{")
	}
}

func (r *Resolver) walkAndResolve(ctx context.Context, node any) (any, error) {
	switch v := node.(type) {
	case string:
		return r.Resolve(ctx, v)
	case map[string]any:
		for key, value := range v {
			resolved, err := r.walkAndResolve(ctx, value)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}

			v[key] = resolved
		}

		return v, nil
	case []any:
		for i, value := range v {
			resolved, err := r.walkAndResolve(ctx, value)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}

			v[i] = resolved
		}

		return v, nil
	default:
		return v, nil
	}
}
