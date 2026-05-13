package kv

import (
	"fmt"
	"sync"
	"time"
)

// cacheEntry holds a cached value with its expiration time
type cacheEntry struct {
	value     string
	expiresAt time.Time
}

// cache provides TTL-based in-memory caching for secret values.
// It's thread-safe and automatically expires entries based on configured TTL.
type cache struct {
	entries map[string]*cacheEntry
	ttl     time.Duration
	enabled bool
	mu      sync.RWMutex
}

func newCache(config CacheConfig) (*cache, error) {
	if !config.Enabled {
		return &cache{enabled: false}, nil
	}

	ttl, err := time.ParseDuration(config.TTL)
	if err != nil {
		return nil, fmt.Errorf("invalid cache TTL %q: %w", config.TTL, err)
	}

	if ttl <= 0 {
		return nil, fmt.Errorf("cache TTL must be positive, got %v", ttl)
	}

	return &cache{
		entries: make(map[string]*cacheEntry),
		enabled: true,
		ttl:     ttl,
	}, nil
}

// TODO: To be implemented
func (c *cache) Get(key string) (string, bool) {
	return "", false
}

// TODO: To be implemented
func (c *cache) Set(key, value string) {}
