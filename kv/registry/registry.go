package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/TykTechnologies/storage/kv"
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
func (r *Registry) Add(providerType string, factory kv.ProviderFactory) error {
	if providerType == "" {
		return errors.New("provider type cannot be empty")
	}

	if factory == nil {
		return errors.New("factory cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Safe check within the write lock prevents the TOCTOU race condition
	if _, ok := r.factories[providerType]; ok {
		return fmt.Errorf("factory for type %q is already provided; override is not allowed", providerType)
	}

	r.factories[providerType] = factory

	return nil
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
//	  "kv": {
//	    "cache": {
//	      "enabled": true,
//	      "ttl": "60s"
//	    },
//	    "stores": {
//	      "vault-prod": {
//	        "type": "vault",
//	        "required": true,
//	        "config": {
//	          "address": "https://vault.internal:8200",
//	          "token": "kv://env/VAULT_TOKEN"
//	        }
//	      }
//	    }
//	  }
//	}
func (r *Registry) InitStores(ctx context.Context, kvConfig *kv.KVConfig) error {
	r.mu.RLock()
	factoriesCount := len(r.factories)
	r.mu.RUnlock()

	if factoriesCount == 0 {
		return errors.New("factories must be added before initialize stores")
	}

	if kvConfig == nil || kvConfig.Stores == nil {
		return nil
	}

	tempStores := make(map[string]kv.Provider)

	for name, storeCfg := range kvConfig.Stores {
		r.mu.RLock()
		factory, ok := r.factories[storeCfg.Type]
		r.mu.RUnlock()

		if !ok {
			err := fmt.Errorf("unknown provider type %q for store %q", storeCfg.Type, name)
			if storeCfg.Required {
				return err
			}
			// FIX: Add the logger...
			// r.logger.WithError(err).Warn("Skipping optional store initialization")
			continue
		}

		provider, err := factory(storeCfg.Config)
		if err != nil {
			initErr := fmt.Errorf("failed to create provider %q (type: %s): %w", name, storeCfg.Type, err)
			if storeCfg.Required {
				return initErr
			}
			// FIX: Add the logger
			// r.logger.WithError(initErr).Warn("Failed to initialize optional store, skipping")
			continue
		}

		if initializer, ok := kv.AsInitializer(provider); ok {
			err := initializer.Init(ctx)
			if err != nil {
				initErr := fmt.Errorf("failed to initialize store %q (type: %s): %w", name, storeCfg.Type, err)
				if storeCfg.Required {
					return initErr
				}

				continue
			}
		}

		// FIX: Uncomment this when PR with cache and stores are merged
		// if storeCfg.type != "env" && storeCfg.type != "inline" {
		// 	timeout := extractTimeout(storeCfg.Config)
		//
		// 	secretStore, err := store.NewSecretStore(
		// 		ctx,
		// 		name,
		// 		provider,
		// 		kvConfig.Cache,
		// 		store.WithTimeout(storeCfg.Config.Timeout)
		// 	)
		// 	if err != nil {
		// 		wrapErr := fmt.Errorf("failed to wrap store %q: %w", name, err)
		// 		if storeCfg.Required {
		// 			return wrapErr
		// 		}
		// 		// log.Warn(...)
		// 		continue
		// 	}
		//
		// 	provider = secretStore
		// }

		tempStores[name] = provider
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for name, provider := range tempStores {
		r.stores[name] = provider
	}

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
	// TODO: We should iterate over stores and call Close.
	// Each provider or secret store will call its Close func
	// which should gracefully shutdown provider connection.
	// AFAIK not every provider need this shutdown so we will type
	// asset for Close method in provider and call it if its present.
	return nil
}

// FIX: Uncomment later
// func extractTimeout(config any) time.Duration {
// 	if config == nil {
// 		return 0
// 	}
//
// 	configMap, ok := config.(map[string]any)
// 	if !ok {
// 		return 0
// 	}
//
// 	timeoutVal, exists := configMap["timeout"]
// 	if !exists {
// 		return 0
// 	}
//
// 	// If parsed from JSON/YAML, it's usually a string ("5s")
// 	if timeoutStr, isString := timeoutVal.(string); isString {
// 		if parsed, err := time.ParseDuration(timeoutStr); err == nil {
// 			return parsed
// 		}
// 	}
//
// 	return 0
// }
