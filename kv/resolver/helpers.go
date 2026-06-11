package resolver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func extractJSONPointer(raw, fragment string) (string, error) {
	// UseNumber keeps numeric leaves as json.Number —  a float64 round-trip
	// silently corrupts integers above 2^53.
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()

	var doc any
	if err := dec.Decode(&doc); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}

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

		switch v := current.(type) {
		case map[string]any:
			val, ok := v[seg]
			if !ok {
				return "", fmt.Errorf("%w: %q", ErrFieldNotFound, seg)
			}

			current = val

		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(v) {
				return "", fmt.Errorf("%w: %q", ErrFieldNotFound, seg)
			}

			current = v[idx]

		default:
			return "", fmt.Errorf("%w: %q", ErrFieldNotFound, seg)
		}
	}

	switch v := current.(type) {
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
