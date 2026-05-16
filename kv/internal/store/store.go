package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/config"
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
}

// Get retrieves a secret value with caching and deduplication.
func (s *SecretStore) Get(ctx context.Context, path string) (string, error) {
	val, exists, needsRefresh, err := s.cache.Get(path)
	if exists {
		// Fail fast on cached errors
		if err != nil {
			return "", err
		}

		// If value is almost expired on cache, the process should refresh it
		// on background which is called "stale-while-revalidate" strategy
		if needsRefresh {
			s.triggerBackgroundRefreshOnce(path)
		}

		return val, err
	}

	ch := s.sf.DoChan(path, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultProviderTimeout)
		defer cancel()

		newVal, err := s.provider.Get(fetchCtx, path)

		// Return earlier to prevent cache poisoning with context errors
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("timeout fetching %q: %w", path, err)
		}

		s.cache.Set(path, newVal, err)

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
			// TODO: Replace with some generic fetch error. If the result is not a string,
			// its a programming mistake as every provider should return strign
			return "", fmt.Errorf("provider returned unexpected type %T; expected string", res)
		}

		return v, nil
	}
}

// Unwrap allows callers to access the underlying provider for optional interfaces (like Lister)
func (s *SecretStore) Unwrap() kv.Provider {
	return s.provider
}

func (s *SecretStore) triggerBackgroundRefreshOnce(path string) {
	// Use separate singleflight key to prevent collision with foreground fetches
	refreshKey := fmt.Sprintf("%s:refresh", path)

	ch := s.sf.DoChan(refreshKey, func() (any, error) {
		return s.doBackgroundRefresh(path)
	})
	_ = ch
}

func (s *SecretStore) doBackgroundRefresh(path string) (any, error) {
	// We're creating a new context for background refresh because we don't want
	// a cancelled HTTP request to abort a cache refresh that benefits all future callers.
	ctx, cancel := context.WithTimeout(context.Background(), defaultProviderTimeout)
	defer cancel()

	newVal, err := s.provider.Get(ctx, path)
	// Update the cache on success to ensure errors don't overwrite valid entries.
	if err == nil {
		s.cache.Set(path, newVal, nil)
	}

	return newVal, nil
}

func NewSecretStore(
	ctx context.Context,
	name string,
	provider kv.Provider,
	cacheConfig config.CacheConfig,
) (*SecretStore, error) {
	if provider == nil {
		return nil, fmt.Errorf("failed to create a secret store with name %q: provider cannot be nil", name)
	}

	cache, err := cache.NewCache(ctx, cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create secret store: %w", err)
	}

	return &SecretStore{
		name:     name,
		provider: provider,
		cache:    cache,
		sf:       &singleflight.Group{},
	}, nil
}
