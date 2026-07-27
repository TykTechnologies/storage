package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/store"

	"golang.org/x/sync/errgroup"
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

	return r
}

// Add registers a provider factory for the given provider type.
func (r *Registry) Add(pt kv.ProviderType, factory kv.ProviderFactory) error {
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
func (r *Registry) InitStores(ctx context.Context, config *kv.Config) (err error) {
	r.mu.RLock()
	factoriesCount := len(r.factories)
	r.mu.RUnlock()

	if factoriesCount == 0 {
		return errors.New("factories must be added before initialize stores")
	}

	if config == nil || config.Stores == nil {
		return nil
	}

	if r.isInitialized.Swap(true) {
		return errors.New("stores have been initialized")
	}

	var tempMu sync.Mutex
	tempStores := make(map[string]kv.Provider, len(config.Stores))

	collect := func(name string, store kv.Provider) {
		tempMu.Lock()
		tempStores[name] = store
		tempMu.Unlock()
	}

	// This defer block guarantees cleanup of partially initialized stores if the
	// overall initialization process fails, preventing resource leaks.
	defer func() {
		if err != nil {
			r.isInitialized.Store(false)

			tempMu.Lock()
			closeStores(ctx, tempStores)
			tempMu.Unlock()
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)

	for name, storeCfg := range config.Stores {
		if scheduleErr := r.scheduleStoreInit(egCtx, eg, name, storeCfg, config.Cache, collect); scheduleErr != nil {
			return scheduleErr
		}
	}

	if err = eg.Wait(); err != nil {
		return err
	}

	return r.commitStores(tempStores)
}

// scheduleStoreInit looks up the factory for a single store and, when found,
// schedules its (potentially blocking) initialization on the errgroup. It
// returns a non-nil error only when a required store cannot be scheduled;
// optional-store failures are logged and swallowed.
func (r *Registry) scheduleStoreInit(
	ctx context.Context,
	eg *errgroup.Group,
	name string,
	storeCfg kv.StoreConfig,
	cacheCfg kv.CacheConfig,
	collect func(name string, store kv.Provider),
) error {
	r.mu.RLock()
	factory, ok := r.factories[storeCfg.Type]
	r.mu.RUnlock()

	if !ok {
		return r.handleStoreInitError(name, storeCfg.Required,
			fmt.Errorf("unknown provider type %q for store %q", storeCfg.Type, name))
	}

	eg.Go(func() error {
		store, initErr := buildSingleStore(ctx, name, storeCfg, cacheCfg, factory)
		if initErr != nil {
			return r.handleStoreInitError(name, storeCfg.Required, initErr)
		}

		collect(name, store)

		return nil
	})

	return nil
}

// handleStoreInitError propagates initialization errors for required stores and
// logs-and-swallows them for optional ones.
func (r *Registry) handleStoreInitError(name string, required bool, err error) error {
	if required {
		return err
	}

	r.logger.Warn("Skipping optional store initialization", map[string]any{
		"store": name,
		"error": err,
	})

	return nil
}

// commitStores publishes the successfully initialized stores into the registry,
// unless the registry was closed while initialization was in flight.
func (r *Registry) commitStores(tempStores map[string]kv.Provider) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check registry wasn't closed during the initialization
	if !r.isInitialized.Load() {
		return errors.New("registry was closed during initialization")
	}

	for name, store := range tempStores {
		r.stores[name] = store
	}

	return nil
}

// closeStores closes any store that implements Closer, using a cancellation-free
// context so cleanup runs even when the original context is already done.
func closeStores(ctx context.Context, stores map[string]kv.Provider) {
	cleanupCtx := context.WithoutCancel(ctx)

	for _, store := range stores {
		if closer, ok := kv.AsCloser(store); ok {
			_ = closer.Close(cleanupCtx)
		}
	}
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
	stores := r.stores
	r.stores = make(map[string]kv.Provider)
	r.isInitialized.Store(false)
	r.mu.Unlock()

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)

	for name, store := range stores {
		wg.Go(func() {
			if closer, ok := kv.AsCloser(store); ok {
				if err := closer.Close(ctx); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("failed to close store %q: %w", name, err))
					mu.Unlock()
				}
			}
		})
	}

	wg.Wait()

	return errors.Join(errs...)
}

func buildSingleStore(
	ctx context.Context,
	name string,
	storeCfg kv.StoreConfig,
	cacheCfg kv.CacheConfig,
	factory kv.ProviderFactory,
) (kv.Provider, error) {
	provider, err := factory(storeCfg.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider %q (type: %s): %w", name, storeCfg.Type, err)
	}

	if initializer, ok := kv.AsInitializer(provider); ok {
		err := initializer.Init(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize store %q (type: %s): %w", name, storeCfg.Type, err)
		}
	}

	if s, ok := kv.AsStandaloner(provider); ok && s.IsStandalone() {
		return provider, nil
	}

	var timeout time.Duration
	if t, ok := kv.AsTimeouter(provider); ok {
		timeout = t.Timeout()
	}

	ss, err := store.NewSecretStore(
		name,
		provider,
		cacheCfg,
		store.WithTimeout(timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to wrap store %q: %w", name, err)
	}

	return ss, nil
}
