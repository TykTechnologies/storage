package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"
)

const defaultProviderTimeout = 5 * time.Second

// SecretStore is the high-level interface. It wraps Provider
// implementations with additional capabilities like caching,
// single-flight deduplication, and enhanced error handling.
type SecretStore interface {
	GetSecret(ctx context.Context, path string) (string, error)
}

type secretStore struct {
	name     string
	provider Provider
	cache    *cache
	sf       *singleflight.Group
}

// GetSecret retrieves a secret value with caching and deduplication.
func (s *secretStore) GetSecret(ctx context.Context, path string) (string, error) {
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

	res, fetchErr, _ := s.sf.Do(path, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(ctx, defaultProviderTimeout)
		defer cancel()

		newVal, err := s.provider.Get(fetchCtx, path)

		if errors.Is(err, context.Canceled) {
			return "", fmt.Errorf("request cancelled while fetching %q: %w", path, err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("timeout fetching %q: %w", path, err)
		}

		s.cache.Set(path, newVal, err)

		return newVal, err
	})

	if fetchErr != nil {
		return "", fetchErr
	}

	// Providers always return string but anyway its better to be safe
	resStr, ok := res.(string)
	if !ok {
		// TODO: Replace with some generic fetch error. If the result is not a string,
		// its a programming mistake as every provider should return strign
		return "", fmt.Errorf("provider returned unexpected type %T; expected string", res)
	}

	return resStr, nil
}

func (s *secretStore) triggerBackgroundRefreshOnce(path string) {
	// Use separate singleflight key to prevent collision with foreground fetches
	refreshKey := fmt.Sprintf("%s:refresh", path)

	ch := s.sf.DoChan(refreshKey, func() (any, error) {
		return s.doBackgroundRefresh(path)
	})
	_ = ch
}

func (s *secretStore) doBackgroundRefresh(path string) (any, error) {
	// We're creating a new context for background refresh because we don't want
	// a cancelled HTTP request to abort a cache refresh that benefits all future callers.
	ctx, cancel := context.WithTimeout(context.Background(), defaultProviderTimeout)
	defer cancel()

	newVal, err := s.provider.Get(ctx, path)

	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.cache.Set(path, newVal, err)
	}

	return newVal, nil
}

func NewSecretStore(
	ctx context.Context,
	name string,
	provider Provider,
	cacheConfig CacheConfig,
) (SecretStore, error) {
	if provider == nil {
		return nil, fmt.Errorf("failed to create a secret store with name %q: provider cannot be nil", name)
	}

	cache, err := newCache(ctx, cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create secret store: %w", err)
	}

	return &secretStore{
		name:     name,
		provider: provider,
		cache:    cache,
		sf:       &singleflight.Group{},
	}, nil
}
