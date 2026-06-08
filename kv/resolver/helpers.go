package resolver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func extractJSONPointer(raw, fragment string) (string, error) {
	var doc any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return "", ErrInvalidJSON
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
				return "", fmt.Errorf("%w: segment %q", ErrFieldNotFound, seg)
			}

			current = val

		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(v) {
				return "", fmt.Errorf("%w: index %q", ErrFieldNotFound, seg)
			}

			current = v[idx]

		default:
			return "", fmt.Errorf(
				"%w: cannot traverse into %T with segment %q",
				ErrFieldNotFound,
				current,
				seg,
			)
		}
	}

	switch v := current.(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
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
