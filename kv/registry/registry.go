package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/store"
)

// Registry manages provider factories and initialized stores without global state.
// It provides a clean separation between provider registration (factories) and
// runtime instances (stores), enabling components to control their own KV lifecycle.
//
// All operations are safe for concurrent use.
type Registry struct {
	factories     map[kv.ProviderType]kv.ProviderFactory
	stores        map[string]kv.Provider
	mu            sync.RWMutex
	isInitialized atomic.Bool
	logger        kv.Logger
}

type Option func(r *Registry)

func WithLogger(l kv.Logger) Option {
	return func(r *Registry) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewRegistry creates a new empty registry with no registered factories or stores.
func NewRegistry(opts ...Option) *Registry {
	r := &Registry{
		factories: make(map[kv.ProviderType]kv.ProviderFactory),
		stores:    make(map[string]kv.Provider),
		logger:    kv.NoopLogger{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// NewDefaultRegistry creates a registry with added OSS providers.
func NewDefaultRegistry(opts ...Option) *Registry {
	r := NewRegistry(opts...)

	// FIX: Uncomment provider registration when implementation is added
	// r.Add("env", env.NewFactory())
	// r.Add("inline", inline.NewFactory())
	// r.Add("hashicorp_vault", vault.NewFactory())
	// r.Add("hashicorp_consul", consul.NewFactory())
	// r.Add("k8s_files", k8s.NewFactory())

	return r
}

// Add registers a provider factory for the given provider type.
func (r *Registry) Add(pt kv.ProviderType, factory kv.ProviderFactory) error {
	// In Go string literals are considered untyped string constants. The compiler
	// won't scream if caller pass the empty string argument as provider type.
	if pt == "" {
		return errors.New("provider type cannot be empty")
	}

	if factory == nil {
		return errors.New("factory cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Safe check within the write lock prevents the TOCTOU race condition
	if _, ok := r.factories[pt]; ok {
		return fmt.Errorf("factory for type %q is already provided; override is not allowed", pt)
	}

	r.factories[pt] = factory

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
func (r *Registry) InitStores(ctx context.Context, kvConfig *kv.Config) (err error) {
	if r.isInitialized.Swap(true) {
		return errors.New("stores have been initialized")
	}

	r.mu.RLock()
	if len(r.factories) == 0 {
		r.mu.RUnlock()
		return errors.New("factories must be added before initialize stores")
	}
	r.mu.RUnlock()

	if kvConfig == nil || kvConfig.Stores == nil {
		r.isInitialized.Store(false)
		return nil
	}

	tempStores := make(map[string]kv.Provider)

	defer func() {
		if err != nil {
			r.isInitialized.Store(false)

			for _, p := range tempStores {
				if closer, ok := kv.AsCloser(p); ok {
					_ = closer.Close(ctx)
				}
			}
		}
	}()

	for name, storeCfg := range kvConfig.Stores {
		r.mu.RLock()
		factory, ok := r.factories[storeCfg.Type]
		r.mu.RUnlock()

		if !ok {
			err = fmt.Errorf("unknown provider type %q for store %q", storeCfg.Type, name)
			if storeCfg.Required {
				return err
			}

			r.logger.Warn("Skipping optional store initialization", map[string]any{
				"err": err,
			})

			continue
		}

		provider, createErr := factory(storeCfg.Config)
		if createErr != nil {
			err = fmt.Errorf("failed to create provider %q (type: %s): %w", name, storeCfg.Type, createErr)
			if storeCfg.Required {
				return err
			}

			r.logger.Warn("Skipping optional store initialization", map[string]any{
				"err": err,
			})

			continue
		}

		if initializer, ok := kv.AsInitializer(provider); ok {
			initError := initializer.Init(ctx)
			if initError != nil {
				err = fmt.Errorf("failed to initialize store %q (type: %s): %w", name, storeCfg.Type, initError)
				if storeCfg.Required {
					return err
				}

				r.logger.Warn("Skipping optional store initialization", map[string]any{
					"err": err,
				})

				continue
			}
		}

		if storeCfg.Type != "env" && storeCfg.Type != "inline" {
			timeout := extractTimeout(storeCfg.Config)

			secretStore, secretStoreErr := store.NewSecretStore(
				ctx,
				name,
				provider,
				kvConfig.Cache,
				store.WithTimeout(timeout),
			)
			if secretStoreErr != nil {
				err = fmt.Errorf("failed to wrap store %q: %w", name, secretStoreErr)
				if storeCfg.Required {
					return err
				}

				r.logger.Warn("Skipping optional store initialization", map[string]any{
					"err": err,
				})

				continue
			}

			provider = secretStore
		}

		tempStores[name] = provider
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// If InitStores and Close were called concurrently we can endup with initialized stores
	// and isInitialized keep false value. The defer func will clear temporaly initialized
	// stores.
	if !r.isInitialized.Load() {
		err = errors.New("registry was closed during initialization")
		return err
	}

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
	r.mu.Lock()
	defer r.mu.Unlock()

	errCh := make(chan error, len(r.stores))
	var wg sync.WaitGroup

	for name, store := range r.stores {
		wg.Go(func() {
			if closer, ok := kv.AsCloser(store); ok {
				if err := closer.Close(ctx); err != nil {
					errCh <- fmt.Errorf("failed to close store %q: %w", name, err)
				}
			}
		})
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	r.stores = make(map[string]kv.Provider)
	r.isInitialized.Store(false)

	return errors.Join(errs...)
}

func extractTimeout(config json.RawMessage) time.Duration {
	if config == nil {
		return 0
	}

	var sc struct {
		Timeout string `json:"timeout"`
	}

	err := json.Unmarshal(config, &sc)
	if err != nil {
		return 0
	}

	if parsed, err := time.ParseDuration(sc.Timeout); err == nil {
		return parsed
	}

	return 0
}
