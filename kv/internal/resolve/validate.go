package resolve

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ValidateSyntax reports whether input is free of malformed KV references,
// without resolving anything or touching a store. It returns nil for a string
// that contains no KV syntax as well as for one whose kv:// and $kv{} references
// are all well-formed; it returns an error wrapping ErrMalformedReference for the
// first malformed reference found.
func ValidateSyntax(input string) error {
	if _, ok, err := parseWholeValue(input); ok {
		return err
	}

	if unclosedInlineToken(input) >= 0 {
		return fmt.Errorf(
			"%w: unclosed $kv{ reference in %q",
			ErrMalformedReference,
			input,
		)
	}

	for _, match := range inlineRe.FindAllString(input, -1) {
		if _, err := parseInlineToken(match); err != nil {
			return err
		}
	}

	return nil
}

// ValidateSyntaxAll walks a raw JSON document and runs ValidateSyntax on every
// string value at any depth, returning the first malformed reference found with
// a field/index breadcrumb. It resolves nothing and contacts no store, so it
// needs no registry.
func ValidateSyntaxAll(rawJSON []byte) error {
	// Fast path: with no KV syntax anywhere there is nothing to validate, and we
	// avoid an unmarshal
	if !bytes.Contains(rawJSON, []byte("kv://")) && !bytes.Contains(rawJSON, []byte("$kv{")) {
		return nil
	}

	dec := json.NewDecoder(bytes.NewReader(rawJSON))
	dec.UseNumber()

	var doc any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidJSON, err)
	}

	return walkAndValidate(doc, "")
}

func walkAndValidate(node any, path string) error {
	switch v := node.(type) {
	case string:
		err := ValidateSyntax(v)
		if err == nil || path == "" {
			return err
		}

		return fmt.Errorf("%s: %w", path, err)
	case map[string]any:
		for key, value := range v {
			if err := walkAndValidate(value, fieldPath(path, key)); err != nil {
				return err
			}
		}

		return nil
	case []any:
		for i, value := range v {
			if err := walkAndValidate(value, indexPath(path, i)); err != nil {
				return err
			}
		}

		return nil
	default:
		return nil
	}
}
