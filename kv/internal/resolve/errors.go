package resolve

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidJSON        = errors.New("invalid JSON")
	ErrFieldNotFound      = errors.New("field not found")
	ErrMalformedReference = errors.New("malformed KV reference")
)

// fieldNotFoundErr wraps ErrFieldNotFound with the JSON pointer segment that
// could not be resolved.
func fieldNotFoundError(field string) error {
	return fmt.Errorf("%w: %q", ErrFieldNotFound, field)
}
