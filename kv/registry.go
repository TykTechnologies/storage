package kv

import (
	"context"
	"sync"
)

// Registry manages provider factories and initialized stores without global state.
// It provides a clean separation between provider registration (factories) and
// runtime instances (stores), enabling components to control their own KV lifecycle.
//
// All operations are safe for concurrent use.
type Registry struct {
	factories map[string]ProviderFactory
	stores    map[string]SecretStore
	mu        sync.RWMutex
}

// NewRegistry creates a new empty registry with no registered factories or stores.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]ProviderFactory),
		stores:    make(map[string]SecretStore),
	}
}

// Add registers a provider factory for the given provider type.
// The providerType should match the "type" field used in store configurations.
//
// Adding a factory with the same providerType will overwrite the previous factory.
func (r *Registry) Add(providerType string, factory ProviderFactory) {}

// InitStores initializes named store instances using registered provider factories.
// The configs map keys become the store names used in KV references.
//
// If a store is marked as required:true and fails to initialize, InitStores
// returns an error. Optional stores (required:false) log warnings but don't
// fail the initialization process.
//
// Example config:
//
//	{
//	  "vault-prod": {"type": "vault", "required": true, "config": {...}},
//	  "aws-dev": {"type": "aws", "required": false, "config": {...}}
//	}
func (r *Registry) InitStores(configs map[string]StoreConfig) error {
	return nil
}

// GetStore retrieves an initialized store by name.
// Returns ErrStoreNotFound if no store with the given name was initialized.
func (r *Registry) GetStore(name string) (SecretStore, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return nil, nil
}

// Close gracefully shuts down all initialized stores.
func (r *Registry) Close(ctx context.Context) error {
	return nil
}
