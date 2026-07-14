package resolver

import (
	"github.com/TykTechnologies/storage/kv/internal/resolve"
)

// Reference is a parsed kv:// whole-value reference of the form
// "kv://<store>/<path>[#<field>]".
type Reference struct {
	// Store is the registry store name
	Store string

	// Path is the provider-specific key or path within the store.
	Path string

	// Field is the optional "#field" fragment; empty when absent.
	Field string
}

// ParseReference parses a kv:// whole-value reference into its parts without
// resolving it — for callers (e.g. a write-back path) that must route to a store
// by name rather than read a value.
//
// ok reports whether s is a kv:// whole-value reference at all. For a string that
// is not one — a legacy scheme (vault://, consul://), a plain literal, or an
// inline "$kv{...}" token — ok is false and err is nil. For a kv:// reference that
// is malformed (missing path separator, empty store or path), ok is true and err
// wraps ErrMalformedReference.
func ParseReference(s string) (Reference, bool, error) {
	store, path, fragment, ok, err := resolve.ParseWholeValue(s)
	if err != nil {
		return Reference{}, ok, err
	}

	if !ok {
		return Reference{}, false, nil
	}

	ref := Reference{
		Store: store,
		Path:  path,
		Field: fragment,
	}

	return ref, true, nil
}
