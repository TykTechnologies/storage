package resolve

import (
	"fmt"
	"strings"
)

// ParseWholeValue is the exported boundary over parseWholeValue for callers
// outside the resolve engine that must parse a kv:// reference without resolving it.
func ParseWholeValue(input string) (store, path, fragment string, ok bool, err error) {
	rk, ok, err := parseWholeValue(input)
	return rk.store, rk.path, rk.fragment, ok, err
}

// refKey identifies a resolution target: a fragment of a secret.
type refKey struct {
	store    string
	path     string
	fragment string
}

// pathKey identifies a backend fetch target — a secret at {store, path},
// independent of any fragment. Fetches are memoized on this so that every
// fragment of the same secret shares a single backend call: the fragment is
// extracted from the fetched value in memory.
type pathKey struct {
	store string
	path  string
}

// fetchKey drops the fragment, yielding the backend fetch target for ref.
func (k refKey) fetchKey() pathKey {
	return pathKey{store: k.store, path: k.path}
}

// distinctPaths collapses a set of references to the distinct backend fetches
// they require, merging multiple fragments of the same secret into one.
func distinctPaths(refs map[refKey]struct{}) map[pathKey]struct{} {
	paths := make(map[pathKey]struct{}, len(refs))
	for ref := range refs {
		paths[ref.fetchKey()] = struct{}{}
	}

	return paths
}

// parseWholeValue parses a whole-value reference of the form
// "kv://store/path#frag".
//
// The bool reports whether input is a whole-value reference at all (i.e. has
// the "kv://" prefix). When false, input is an inline/literal string and err is
// nil. When true, err is non-nil if the reference is malformed.
func parseWholeValue(input string) (refKey, bool, error) {
	if !strings.HasPrefix(input, "kv://") {
		return refKey{}, false, nil
	}

	trimmed := strings.TrimPrefix(input, "kv://")

	slashIdx := strings.IndexByte(trimmed, '/')
	if slashIdx < 0 {
		return refKey{}, true, fmt.Errorf(
			"%w: missing path separator in %q",
			ErrMalformedReference,
			input,
		)
	}

	store := trimmed[:slashIdx]
	path, fragment, _ := strings.Cut(trimmed[slashIdx+1:], "#")

	if store == "" || path == "" {
		return refKey{}, true, fmt.Errorf(
			"%w: empty store name or path in %q",
			ErrMalformedReference,
			input,
		)
	}

	return refKey{store: store, path: path, fragment: fragment}, true, nil
}

// parseInlineToken parses a single inline token. match is the full "$kv{...}"
// text (used verbatim in error messages); its contents are "store:path#fragment".
func parseInlineToken(match string) (refKey, error) {
	// strip "$kv{" prefix and "}" suffix
	inner := match[len("$kv{") : len(match)-1]

	colonIdx := strings.IndexByte(inner, ':')
	if colonIdx < 0 {
		return refKey{}, fmt.Errorf(
			"%w: missing store separator in %q",
			ErrMalformedReference,
			match,
		)
	}

	store := inner[:colonIdx]
	path, fragment, _ := strings.Cut(inner[colonIdx+1:], "#")

	if store == "" || path == "" {
		return refKey{}, fmt.Errorf(
			"%w: empty store name or path in %q",
			ErrMalformedReference,
			match,
		)
	}

	return refKey{store: store, path: path, fragment: fragment}, nil
}

// collectRefs walks a decoded JSON document and returns every distinct,
// well-formed reference target. It is best-effort: malformed references are
// skipped here.
func collectRefs(node any) map[refKey]struct{} {
	refs := make(map[refKey]struct{})
	collectInto(node, refs)

	return refs
}

func collectInto(node any, into map[refKey]struct{}) {
	switch v := node.(type) {
	case string:
		collectRefsFromString(v, into)
	case map[string]any:
		for _, value := range v {
			collectInto(value, into)
		}
	case []any:
		for _, value := range v {
			collectInto(value, into)
		}
	}
}

func collectRefsFromString(input string, into map[refKey]struct{}) {
	if ref, ok, err := parseWholeValue(input); ok {
		if err == nil {
			into[ref] = struct{}{}
		}

		return
	}

	for _, match := range inlineRe.FindAllString(input, -1) {
		if ref, err := parseInlineToken(match); err == nil {
			into[ref] = struct{}{}
		}
	}
}
