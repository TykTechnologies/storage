package resolver

import (
	"context"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
)

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
	// reference — everything after the store name is the path, including any
	// "$kv{...}" text, which is NOT expanded. Inline tokens are only processed
	// in strings that do not start with "kv://". An inline path cannot contain
	// "}" — use the whole-value form for such keys.
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
	// If the input is not valid JSON, ErrInvalidJSON is returned. A valid
	// document containing no KV syntax (no "kv://" or "$kv{" substrings) is
	// returned byte-for-byte unchanged. Documents that do contain KV syntax
	// are re-serialized: the output normalizes formatting (object keys sorted,
	// insignificant whitespace removed) while preserving all values.
	// HTML characters (&, <, >) are NOT escaped in the output.
	ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error)
}

// NewResolver returns a Resolver that resolves references against the given
// store getter (typically *registry.Registry). Unresolvable references are
// always errors.
func NewResolver(registry kv.StoreGetter) Resolver {
	return resolve.NewResolver(registry)
}
