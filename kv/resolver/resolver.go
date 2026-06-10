package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/TykTechnologies/storage/kv"
)

type StoreGetter interface {
	GetStore(name string) (kv.Provider, error)
}

// Resolver handles string replacement for KV references in configuration strings.
// It supports two syntax patterns:
//   - Whole-value references: "kv://store-name/path/to/secret#field"
//   - Inline references: "https://$kv{store-name:path/to/secret#field}/api/v1"
//
// The resolver works against a registry of named stores, allowing the same
// syntax to work across different provider types (Vault, Consul, AWS, etc.).
//
// JSON field extraction is supported via the #field syntax using JSON Pointer
// notation for nested field access.
type Resolver interface {
	// Resolve processes the input string and replaces any KV references with
	// their resolved values from the configured stores.
	//
	// Returns the resolved string with all KV references replaced, or an error
	// if any reference cannot be resolved.
	//
	// If the input contains no KV references, it is returned unchanged.
	Resolve(ctx context.Context, input string) (string, error)

	// ResolveAll walks a raw JSON document recursively and applies Resolve to
	// every string value found at any depth, including inside nested objects
	// and arrays. Non-string scalars (numbers, booleans, null) are left as-is.
	//
	// Returns the re-serialized document with all KV references replaced, or an
	// error if any reference cannot be resolved. On error the document is not
	// partially written.
	//
	// If the input is not valid JSON, ErrInvalidJSON is returned.
	ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error)
}

type resolver struct {
	registry StoreGetter
}

func NewResolver(registry StoreGetter) Resolver {
	return &resolver{registry: registry}
}

var inlineRe = regexp.MustCompile(`\$kv\{([^}]+)\}`)

func (r *resolver) Resolve(ctx context.Context, input string) (string, error) {
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

		return r.fetchAndExtract(ctx, storeName, path, fragment)
	}

	var resolveErr error
	result := inlineRe.ReplaceAllStringFunc(input, func(match string) string {
		if resolveErr != nil {
			return match
		}

		// Strip "kv{}"
		inner := match[4 : len(match)-1]

		colonIdx := strings.IndexByte(inner, ':')
		if colonIdx < 0 {
			resolveErr = fmt.Errorf(
				"%w: missing store separator in %q",
				ErrMalformedReference,
				match,
			)

			return match
		}

		storeName := inner[:colonIdx]
		rest := inner[colonIdx+1:]
		path, fragment, _ := strings.Cut(rest, "#")

		val, err := r.fetchAndExtract(ctx, storeName, path, fragment)
		if err != nil {
			resolveErr = err
			return match
		}

		return val
	})

	if resolveErr != nil {
		return "", resolveErr
	}

	return result, nil
}

func (r *resolver) ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error) {
	// Fast path: skip unmarshal/remarshal entirely when no KV syntax is present,
	// preserving the original bytes and avoiding unnecessary allocations.
	if !bytes.Contains(rawJSON, []byte("kv://")) && !bytes.Contains(rawJSON, []byte("$kv{")) {
		return rawJSON, nil
	}

	var doc any
	if err := json.Unmarshal(rawJSON, &doc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}

	resolved, err := r.walkAndResolve(ctx, doc)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resolved)
}

func (r *resolver) fetchAndExtract(ctx context.Context, storeName, path, fragment string) (string, error) {
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

func (r *resolver) walkAndResolve(ctx context.Context, node any) (any, error) {
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
