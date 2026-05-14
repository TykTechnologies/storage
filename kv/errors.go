package kv

import (
	"errors"
	"fmt"
)

var (
	// ErrStoreNotFound is returned when referencing an unregistered store name.
	ErrStoreNotFound = errors.New("store not found")

	// ErrProviderNotFound is returned when no factory is registered for a provider type.
	ErrProviderNotFound = errors.New("provider factory not found")

	// ErrFieldNotFound is returned when JSON field extraction fails.
	ErrFieldNotFound = errors.New("field not found in JSON")

	// ErrInvalidReference is returned when KV reference syntax is malformed.
	ErrInvalidReference = errors.New("invalid KV reference syntax")

	// ErrStoreRequired is returned when a required store fails to initialize.
	ErrStoreRequired = errors.New("required store failed to initialize")
)

func NewStoreNotFoundError(storeName string) error {
	return fmt.Errorf("store %q: %w", storeName, ErrStoreNotFound)
}

func NewProviderNotFoundError(providerType string) error {
	return fmt.Errorf("provider type %q: %w", providerType, ErrProviderNotFound)
}

func NewFieldNotFoundError(field, jsonValue string) error {
	return fmt.Errorf("field %q in JSON %q: %w", field, jsonValue, ErrFieldNotFound)
}

func NewInvalidReferenceError(reference string) error {
	return fmt.Errorf("reference %q: %w", reference, ErrInvalidReference)
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
