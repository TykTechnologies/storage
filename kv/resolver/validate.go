package resolver

import (
	"github.com/TykTechnologies/storage/kv/internal/resolve"
)

// ValidateSyntax reports whether s is free of malformed KV references, without
// resolving anything or contacting a store. It returns nil for a string with no
// KV syntax and for one whose kv:// and $kv{} references are all well-formed; it
// returns an error wrapping ErrMalformedReference for the first malformed
// reference found.
func ValidateSyntax(s string) error {
	return resolve.ValidateSyntax(s)
}

// ValidateSyntaxAll walks a raw JSON document and validates the KV reference
// syntax of every string value at any depth, without resolving anything or
// contacting a store. It returns nil when every reference (in any field, not
// only URL fields) is well-formed, an error wrapping ErrMalformedReference for
// the first malformed reference, or ErrInvalidJSON if the document cannot be
// parsed.
func ValidateSyntaxAll(rawJSON []byte) error {
	return resolve.ValidateSyntaxAll(rawJSON)
}
