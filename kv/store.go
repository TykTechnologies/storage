package kv

import (
	"context"
	"fmt"
)

// SecretStore is the high-level interface. It wraps Provider
// implementations with additional capabilities like caching,
// single-flight deduplication, and enhanced error handling.
type SecretStore interface {
	GetSecret(ctx context.Context, path string) (string, error)
}

// TODO: Add single flight field with implementation
type secretStore struct {
	name     string
	provider Provider
	cache    *cache
}

func NewSecretStore(name string, provider Provider, cacheConfig CacheConfig) (SecretStore, error) {
	if provider == nil {
		return nil, fmt.Errorf("failed to create a secret store with name '%s': provider cannot be nil", name)
	}

	cache, err := newCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create secret store: %w", err)
	}

	return &secretStore{
		name:     name,
		provider: provider,
		cache:    cache,
	}, nil
}

// GetSecret retrieves a secret value with caching and deduplication.
func (s *secretStore) GetSecret(ctx context.Context, path string) (string, error) {
	return "", nil
}
