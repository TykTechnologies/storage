package resolve

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func extractJSONPointer(raw, fragment string) (string, error) {
	// UseNumber keeps numeric leaves as json.Number — a float64 round-trip
	// silently corrupts integers above 2^53.
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()

	var doc any
	if err := dec.Decode(&doc); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}

	node, err := walkJSONPointer(doc, fragment)
	if err != nil {
		return "", err
	}

	return stringifyLeaf(node)
}

func walkJSONPointer(doc any, fragment string) (any, error) {
	// Normalize fragment with leading "/"
	if !strings.HasPrefix(fragment, "/") {
		fragment = "/" + fragment
	}

	// Split on "/" and skip first empty segment
	segments := strings.Split(fragment, "/")[1:]

	current := doc

	for _, seg := range segments {
		// Unescape RFC 6901: ~1 → /, ~0 → ~  (order matters)
		seg = strings.ReplaceAll(seg, "~1", "/")
		seg = strings.ReplaceAll(seg, "~0", "~")

		next, err := pointerChild(current, seg)
		if err != nil {
			return nil, err
		}

		current = next
	}

	return current, nil
}

func pointerChild(node any, seg string) (any, error) {
	switch v := node.(type) {
	case map[string]any:
		val, ok := v[seg]
		if !ok {
			return nil, fieldNotFoundError(seg)
		}

		return val, nil

	case []any:
		idx, err := strconv.Atoi(seg)
		if err != nil || idx < 0 || idx >= len(v) {
			return nil, fieldNotFoundError(seg)
		}

		return v[idx], nil

	default:
		return nil, fieldNotFoundError(seg)
	}
}

func stringifyLeaf(node any) (string, error) {
	switch v := node.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "null", nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}

		return string(b), nil
	}
}
