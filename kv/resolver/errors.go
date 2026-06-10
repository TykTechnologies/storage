package resolver

import "errors"

var (
	ErrInvalidJSON   = errors.New("invalid JSON")
	ErrFieldNotFound = errors.New("field not found")
)
