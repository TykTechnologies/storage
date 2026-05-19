package store

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/cache"
	"golang.org/x/sync/singleflight"
)

const defaultProviderTimeout = 5 * time.Second

// SecretStore is an internal decorator that adds caching and singleflight to a Provider.
type SecretStore struct {
	name     string
	provider kv.Provider
	cache    *cache.Cache
	sf       *singleflight.Group
	isClosed atomic.Bool
	timeout  time.Duration
}

// Option defines a functional option for configuring the SecretStore.
type Option func(*SecretStore)

// Get retrieves a secret value with caching and deduplication.
func (s *SecretStore) Get(ctx context.Context, path string) (string, error) {
	if s.isClosed.Load() {
		return "", kv.ErrStoreClosed
	}

	val, exists, needsRefresh, err := s.cache.Get(path)
	if exists {
		// Fail fast on cached errors
		if err != nil {
			return "", err
		}

		// If value is almost expired on cache, the process should refresh it
		// on background which is called "stale-while-revalidate" strategy
		if needsRefresh && !s.isClosed.Load() {
			s.triggerBackgroundRefreshOnce(path)
		}

		return val, err
	}

	if s.isClosed.Load() {
		return "", kv.ErrStoreClosed
	}

	ch := s.sf.DoChan(path, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
		defer cancel()

		newVal, err := s.provider.Get(fetchCtx, path)

		// Return earlier to prevent cache poisoning with context errors
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("timeout fetching %q: %w", path, err)
		}

		if !s.isClosed.Load() {
			s.cache.Set(path, newVal, err)
		}

		return newVal, err
	})

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}

		// Providers always return string
		v, ok := res.Val.(string)
		if !ok {
			return "", fmt.Errorf(
				"%w: path %q returned non-string type",
				kv.ErrContractViolation,
				path,
			)
		}

		return v, nil
	}
}

// Unwrap allows callers to access the underlying provider for optional interfaces (like Lister)
func (s *SecretStore) Unwrap() kv.Provider {
	return s.provider
}

func (s *SecretStore) Close(ctx context.Context) error {
	if s.isClosed.Swap(true) {
		return nil
	}

	s.cache.Close()

	if closer, ok := kv.AsCloser(s.provider); ok {
		return closer.Close(ctx)
	}

	return nil
}

func (s *SecretStore) triggerBackgroundRefreshOnce(path string) {
	// Use separate singleflight key to prevent collision with foreground fetches
	refreshKey := path + ":refresh"

	ch := s.sf.DoChan(refreshKey, func() (any, error) {
		return s.doBackgroundRefresh(path)
	})
	_ = ch
}

func (s *SecretStore) doBackgroundRefresh(path string) (any, error) {
	if s.isClosed.Load() {
		return "", kv.ErrStoreClosed
	}

	// We're creating a new context for background refresh because we don't want
	// a cancelled HTTP request to abort a cache refresh that benefits all future callers.
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	newVal, err := s.provider.Get(ctx, path)
	// Update the cache on success to ensure errors don't overwrite valid entries.
	if err == nil && !s.isClosed.Load() {
		s.cache.Set(path, newVal, nil)
	}

	return newVal, err
}

// WithTimeout overrides the global default provider timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(store *SecretStore) {
		if timeout > 0 {
			store.timeout = timeout
		}
	}
}

// NewSecretStore instantiates the store wrapper with optional configurations.
func NewSecretStore(
	ctx context.Context,
	name string,
	provider kv.Provider,
	cacheConfig kv.CacheConfig,
	opts ...Option,
) (*SecretStore, error) {
	if provider == nil {
		return nil, fmt.Errorf("failed to create a secret store with name %q: provider cannot be nil", name)
	}

	cache, err := cache.NewCache(ctx, cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create secret store: %w", err)
	}

	s := &SecretStore{
		name:     name,
		provider: provider,
		cache:    cache,
		sf:       &singleflight.Group{},
		timeout:  defaultProviderTimeout,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}
