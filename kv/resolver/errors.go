package resolver

import "errors"

var (
	ErrInvalidJSON   = errors.New("payload is not valid JSON")
	ErrFieldNotFound = errors.New("field not found in JSON payload")
)
