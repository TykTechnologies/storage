package registry

import (
	"context"
	"sync"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/config"
)

// Registry manages provider factories and initialized stores without global state.
// It provides a clean separation between provider registration (factories) and
// runtime instances (stores), enabling components to control their own KV lifecycle.
//
// All operations are safe for concurrent use.
type Registry struct {
	factories map[string]kv.ProviderFactory
	stores    map[string]kv.Provider
	mu        sync.RWMutex
}

// NewRegistry creates a new empty registry with no registered factories or stores.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]kv.ProviderFactory),
		stores:    make(map[string]kv.Provider),
	}
}

// Add registers a provider factory for the given provider type.
// The providerType should match the "type" field used in store configurations.
//
// Adding a factory with the same providerType will overwrite the previous factory.
func (r *Registry) Add(providerType string, factory kv.ProviderFactory) {
	r.mu.Lock()
	r.factories[providerType] = factory
	r.mu.Unlock()
}

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
func (r *Registry) InitStores(configs map[string]config.StoreConfig) error {
	return nil
}

// GetStore retrieves an initialized store by name.
// Returns ErrStoreNotFound if no store with the given name was initialized.
func (r *Registry) GetStore(name string) (kv.Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	store, ok := r.stores[name]
	if !ok {
		return nil, kv.NewStoreNotFoundError(name)
	}

	return store, nil
}

// Close gracefully shuts down all initialized stores.
func (r *Registry) Close(ctx context.Context) error {
	return nil
}
