package temperr

import "errors"

var (
	// Connection related errors
	InvalidConnector     = errors.New("invalid connector")
	InvalidOptionsType   = errors.New("invalid configuration options type")
	InvalidHandlerType   = errors.New("invalid handler type")
	InvalidConfiguration = errors.New("invalid configuration")

	// Key related errors
	KeyNotFound = errors.New("key not found")
	KeyEmpty    = errors.New("key cannot be empty")
	KeyMisstype = errors.New("invalid operation for key type")
)