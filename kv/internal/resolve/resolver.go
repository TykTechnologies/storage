package resolve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/TykTechnologies/storage/kv"
)

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
	if strings.HasPrefix(input, "kv://") {
		return r.resolveURI(ctx, input)
	}

	return r.resolveInline(ctx, input)
}

// resolveURI resolves a whole-value "kv://store/path#fragment" reference.
func (r *Resolver) resolveURI(ctx context.Context, input string) (string, error) {
	body := strings.TrimPrefix(input, "kv://")

	storeName, path, fragment, err := parseReference(body, '/', input)
	if err != nil {
		return "", err
	}

	res, err := r.fetchAndExtract(ctx, storeName, path, fragment)
	if r.lenient && errors.Is(err, kv.ErrStoreNotFound) {
		return input, nil
	}

	return res, err
}

// resolveInline resolves every embedded "$kv{store:path#fragment}" token in a
// larger string, accumulating errors so a single call reports all failures.
func (r *Resolver) resolveInline(ctx context.Context, input string) (string, error) {
	// The token regex requires a closing brace, so an unclosed "$kv{" can
	// never match — without this check a typo'd reference would silently pass
	// through as a literal value.
	if idx := unclosedInlineToken(input); idx >= 0 {
		return "", fmt.Errorf(
			"%w: unclosed $kv{ reference in %q",
			ErrMalformedReference,
			input,
		)
	}

	var resolveErrs []error

	result := inlineRe.ReplaceAllStringFunc(input, func(match string) string {
		val, err := r.resolveInlineToken(ctx, match)
		if err != nil {
			resolveErrs = append(resolveErrs, err)
		}

		return val
	})

	if len(resolveErrs) > 0 {
		return "", errors.Join(resolveErrs...)
	}

	return result, nil
}

func (r *Resolver) resolveInlineToken(ctx context.Context, match string) (string, error) {
	// strip "$kv{" prefix and "}" suffix
	inner := match[4 : len(match)-1]

	storeName, path, fragment, err := parseReference(inner, ':', match)
	if err != nil {
		return match, err
	}

	val, err := r.fetchAndExtract(ctx, storeName, path, fragment)
	if err != nil {
		if r.lenient && errors.Is(err, kv.ErrStoreNotFound) {
			return match, nil
		}

		return match, err
	}

	return val, nil
}

// parseReference splits "store<sep>path#fragment" into its parts. raw is the
// original reference text, used only for error messages.
func parseReference(body string, sep byte, raw string) (storeName, path, fragment string, err error) {
	sepIdx := strings.IndexByte(body, sep)
	if sepIdx < 0 {
		return "", "", "", fmt.Errorf(
			"%w: missing store/path separator in %q",
			ErrMalformedReference,
			raw,
		)
	}

	storeName = body[:sepIdx]
	path, fragment, _ = strings.Cut(body[sepIdx+1:], "#")

	if storeName == "" || path == "" {
		return "", "", "", fmt.Errorf(
			"%w: empty store name or path in %q",
			ErrMalformedReference,
			raw,
		)
	}

	return storeName, path, fragment, nil
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

func (r *Resolver) fetchAndExtract(ctx context.Context, storeName, path, fragment string) (string, error) {
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
