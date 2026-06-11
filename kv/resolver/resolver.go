package resolver

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
	// if any reference cannot be resolved. When several inline tokens fail,
	// all failures are reported in a single joined error.
	//
	// If the input contains no KV references, it is returned unchanged.
	//
	// Precedence: an input starting with "kv://" is treated as a whole-value
	// reference —  everything after the store name is the path, including any
	// "$kv{...}" text, which is NOT expanded. Inline tokens are only processed
	// in strings that do not start with "kv://". An inline path cannot contain
	// "}" —  use the whole-value form for such keys.
	Resolve(ctx context.Context, input string) (string, error)

	// ResolveAll walks a raw JSON document recursively and applies Resolve to
	// every string value found at any depth, including inside nested objects
	// and arrays. Non-string scalars (numbers, booleans, null) are left as-is;
	// number values preserve their exact representation (no float64 precision
	// loss for large integers).
	//
	// Returns the re-serialized document with all KV references replaced, or an
	// error if any reference cannot be resolved. On error the document is not
	// partially written.
	//
	// A document containing no KV syntax (no "kv://" or "$kv{" substrings) is
	// returned byte-for-byte unchanged WITHOUT being parsed — invalid JSON
	// passes through in that case. Documents that do contain KV syntax are
	// parsed; ErrInvalidJSON is returned if they are not valid JSON, and the
	// re-serialized output normalizes formatting (object keys sorted,
	// insignificant whitespace removed) while preserving all values.
	// HTML characters (&, <, >) are NOT escaped in the output.
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

		if storeName == "" || path == "" {
			return "", fmt.Errorf(
				"%w: empty store name or path in %q",
				ErrMalformedReference,
				input,
			)
		}

		return r.fetchAndExtract(ctx, storeName, path, fragment)
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

func (r *resolver) ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error) {
	// Fast path: skip unmarshal/remarshal entirely when no KV syntax is present,
	// preserving the original bytes and avoiding unnecessary allocations.
	// NOTE: this means documents without KV syntax are NOT validated as JSON.
	if !bytes.Contains(rawJSON, []byte("kv://")) && !bytes.Contains(rawJSON, []byte("$kv{")) {
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
