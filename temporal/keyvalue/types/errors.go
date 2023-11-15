package types

import "errors"

var (
	ErrInvalidConnector = errors.New("you are trying to use an invalid connector")
	ErrKeyNotFound      = errors.New("key not found")
	ErrKeyNotEmpty      = errors.New("key cannot be empty")
)
