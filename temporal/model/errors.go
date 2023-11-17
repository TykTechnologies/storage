package model

import "errors"

var (
	ErrInvalidConnector = errors.New("invalid connector")
	ErrKeyNotFound      = errors.New("key not found")
	ErrKeyEmpty         = errors.New("key cannot be empty")
	ErrKeyMisstype      = errors.New("invalid operation for key type")

	ErrInvalidOptionsType   = errors.New("invalid configuration options type")
	ErrInvalidHandlerType   = errors.New("invalid handler type")
	ErrInvalidConfiguration = errors.New("invalid configuration")
)
