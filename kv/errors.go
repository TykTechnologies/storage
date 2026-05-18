package kv

import (
	"errors"
	"fmt"
)

var (
	// ErrStoreNotFound is returned when referencing an unregistered store name.
	ErrStoreNotFound = errors.New("store not found")

	// ErrContractViolation indicates that an underlying KV provider returned data
	// or behavior that violates the expected API contract (e.g., type assertion failures,
	// missing required metadata, or structural corruption).
	//
	// This represents an invariant failure or programming mistake within the provider
	// implementation rather than a transient operational issue like a network timeout.
	ErrContractViolation = errors.New("provider contract violation")
)

func NewStoreNotFoundError(storeName string) error {
	return fmt.Errorf("store %q: %w", storeName, ErrStoreNotFound)
}

// KeyNotFoundError indicates the store is reachable but the key does not exist.
type KeyNotFoundError struct {
	StoreName string
	KeyPath   string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key %q not found in store %q", e.KeyPath, e.StoreName)
}

// StoreUnavailableError indicates a transient failure reaching the store.
type StoreUnavailableError struct {
	StoreName string
	KeyPath   string
	Err       error
}

func (e *StoreUnavailableError) Error() string {
	return fmt.Sprintf("store %q unavailable when fetching key %q: %v", e.StoreName, e.KeyPath, e.Err)
}

func (e *StoreUnavailableError) Unwrap() error {
	return e.Err
}
