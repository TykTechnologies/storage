package types

import "errors"

var (
	ErrInvalidOptionsType   = errors.New("invalid configuration options type")
	ErrInvalidHandlerType   = errors.New("invalid handler type")
	ErrInvalidConfiguration = errors.New("invalid configuration")
	ErrInvalidConnector     = errors.New("you are trying to use an invalid connector")
	ErrKeyNotFound          = errors.New("key not found")
	ErrKeyNotEmpty          = errors.New("key cannot be empty")
)
