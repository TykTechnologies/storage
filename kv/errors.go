package kv

import (
	"errors"
	"fmt"
)

var (
	// ErrKeyNotFound is returned when a requested key doesn't exist in the store.
	ErrKeyNotFound = errors.New("key not found")

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

func NewKeyNotFoundError(store, key string) error {
	return fmt.Errorf("store %q key %q: %w", store, key, ErrKeyNotFound)
}

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
