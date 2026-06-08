package resolver

import (
	"context"
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

	ResolveAll(ctx context.Context, rawJSON []byte) ([]byte, error)
}

type resolver struct {
	registry StoreGetter
	logger   kv.Logger
}

func NewResolver(registry StoreGetter, logger kv.Logger) Resolver {
	return &resolver{registry: registry, logger: logger}
}

var inlineRe = regexp.MustCompile(`\$kv\{([^}]+)\}`)

// input like - kv://store-name/some-stuff#field
// This method should parse it and detect the kv:// or $kv{
// It should detect the 3 parts of the string, store-name, key and field within value if provided.
// To detect this parts we need some regex probably.
// When we get the parts, we should validate them.
// When every values is validated we're trying to get store first, check if its present,
// return error if its not.
// If the field is present we should JSON unmarshal the value and search for the field. Return error
// if its not present OR if its empty. I have to clarify this with Andy but it looks logical to me
// that if the value is empty something is wrong and the client should know that they put empty value.
// Warning maeby?
// If an input is inline we have to inject result to the string and return complete result
func (r *resolver) Resolve(ctx context.Context, input string) (string, error) {
	if strings.HasPrefix(input, "kv://") {
		trimmed := strings.TrimPrefix(input, "kv://")

		slashIdx := strings.IndexByte(trimmed, '/')
		if slashIdx < 0 {
			return "", fmt.Errorf("malformed kv:// reference: %q", input)
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
			resolveErr = fmt.Errorf("malformed $kv{} token: %q", match)
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
	return nil, nil
}

func (r *resolver) fetchAndExtract(ctx context.Context, storeName, path, fragment string) (string, error) {
	store, err := r.registry.GetStore(storeName)
	if err != nil {
		return "", fmt.Errorf("get store %q: %w", storeName, err)
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
