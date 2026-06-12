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
		trimmed := strings.TrimPrefix(input, "kv://")

		slashIdx := strings.IndexByte(trimmed, '/')
		if slashIdx < 0 {
			return "", fmt.Errorf(
				"%w: missing path separator in %q",
				ErrMalformedReference,
				input,
			)
		}

		storeName := trimmed[:slashIdx]
		rest := trimmed[slashIdx+1:]
		path, fragment, _ := strings.Cut(rest, "#")

		if storeName == "" || path == "" {
			return "", fmt.Errorf(
				"%w: empty store name or path in %q",
				ErrMalformedReference,
				input,
			)
		}

		res, err := r.fetchAndExtract(ctx, storeName, path, fragment)
		if r.lenient && errors.Is(err, kv.ErrStoreNotFound) {
			return input, nil
		}

		return res, err
	}

	var resolveErrs []error
	result := inlineRe.ReplaceAllStringFunc(input, func(match string) string {
		// strip "$kv{" prefix and "}" suffix
		inner := match[4 : len(match)-1]

		colonIdx := strings.IndexByte(inner, ':')
		if colonIdx < 0 {
			resolveErrs = append(resolveErrs, fmt.Errorf(
				"%w: missing store separator in %q",
				ErrMalformedReference,
				match,
			))

			return match
		}

		storeName := inner[:colonIdx]
		rest := inner[colonIdx+1:]
		path, fragment, _ := strings.Cut(rest, "#")

		if storeName == "" || path == "" {
			resolveErrs = append(resolveErrs, fmt.Errorf(
				"%w: empty store name or path in %q",
				ErrMalformedReference,
				match,
			))

			return match
		}

		val, err := r.fetchAndExtract(ctx, storeName, path, fragment)
		if err != nil {
			if r.lenient && errors.Is(err, kv.ErrStoreNotFound) {
				return match
			}

			resolveErrs = append(resolveErrs, err)
			return match
		}

		return val
	})

	if len(resolveErrs) > 0 {
		return "", errors.Join(resolveErrs...)
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
