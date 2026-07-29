package resolve

import (
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
