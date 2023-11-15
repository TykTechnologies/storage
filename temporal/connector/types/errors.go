package types

import "errors"

var (
	ErrInvalidOptionsType   = errors.New("invalid configuration options type")
	ErrInvalidHandlerType   = errors.New("invalid handler type")
	ErrInvalidConfiguration = errors.New("invalid configuration")
)
