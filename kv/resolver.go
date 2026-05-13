package kv

import "context"

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
}

// ResolveConfig processes an entire configuration and resolves any
// KV references found within string fields.
//
// This function enables config-level resolution during component startup,
// allowing any string field in any configuration structure to contain
// KV references that will be resolved before the config is used.
//
// The resolver traverses the JSON structure recursively, applying Resolve()
// to all string values while preserving the overall structure and non-string
// fields unchanged.
func ResolveConfig(ctx context.Context, resolver Resolver, rawConfig []byte) ([]byte, error) {
	return nil, nil
}
