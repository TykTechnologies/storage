package kv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	defaultNegativeTTLNotFound  = 60 * time.Second
	defaultNegativeTTLTransient = 5 * time.Second
)

// cacheEntry holds a cached value with its expiration time
type cacheEntry struct {
	value     string
	err       error
	expiresAt time.Time
}

// cache provides TTL-based in-memory caching for secret values.
// It's thread-safe and automatically expires entries based on configured TTL.
type cache struct {
	entries              map[string]*cacheEntry
	enabled              bool
	ttl                  time.Duration
	refreshBeforeExpiry  time.Duration
	negativeTTLNotFound  time.Duration
	negativeTTLTransient time.Duration
	mu                   sync.RWMutex
}

func (c *cache) Get(key string) (string, bool, bool, error) {
	if !c.enabled {
		return "", false, false, nil
	}

	entry, exists, expired := c.get(key)
	if !exists || expired {
		return "", false, false, nil
	}

	var needsRefresh bool
	if c.refreshBeforeExpiry > 0 && time.Until(entry.expiresAt) <= c.refreshBeforeExpiry {
		needsRefresh = true
	}

	return entry.value, true, needsRefresh, entry.err
}

func (c *cache) Set(key, value string, err error) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	ttl := c.ttl

	if err != nil {
		var notFoundErr *KeyNotFoundError

		if errors.As(err, &notFoundErr) {
			ttl = c.negativeTTLNotFound
		} else {
			ttl = c.negativeTTLTransient
		}
	}

	c.entries[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		err:       err,
	}
}

func (c *cache) get(key string) (*cacheEntry, bool, bool) {
	now := time.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]

	var expired bool
	if entry != nil && now.After(entry.expiresAt) {
		expired = true
	}

	return entry, exists, expired
}

func (c *cache) cleanupLoop(ctx context.Context) {
	interval := c.ttl
	if interval < time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

func (c *cache) cleanup() {
	now := time.Now()
	var keysToDelete []string

	c.mu.RLock()
	for k, v := range c.entries {
		// Delete expired entries. !Before() handles both now > expiresAt and now == expiresAt
		if v == nil || !now.Before(v.expiresAt) {
			keysToDelete = append(keysToDelete, k)
		}
	}
	c.mu.RUnlock()

	if len(keysToDelete) == 0 {
		return
	}

	c.mu.Lock()
	for _, k := range keysToDelete {
		delete(c.entries, k)
	}
	c.mu.Unlock()
}

func newCache(ctx context.Context, config CacheConfig) (*cache, error) {
	if !config.Enabled {
		return &cache{enabled: false, entries: make(map[string]*cacheEntry)}, nil
	}

	ttl, err := time.ParseDuration(config.TTL)
	if err != nil {
		return nil, fmt.Errorf("invalid cache ttl %q: %w", config.TTL, err)
	}

	if ttl <= 0 {
		return nil, fmt.Errorf("cache ttl must be positive, got %v", config.TTL)
	}

	var refreshBeforeExpiry time.Duration

	if config.RefreshBeforeExpiry != "" {
		var err error

		refreshBeforeExpiry, err = time.ParseDuration(config.RefreshBeforeExpiry)
		if err != nil {
			return nil, fmt.Errorf("invalid cache refresh_before_expiry value: %w", err)
		}

		if refreshBeforeExpiry < 0 {
			return nil, fmt.Errorf("cache refresh_before_expiry must be positive, got %v", config.RefreshBeforeExpiry)
		}

		if refreshBeforeExpiry >= ttl {
			refreshBeforeExpiry = 0

			// TODO: Replace with logger
			log.Printf(
				"refresh_before_expiry of %q is not less than ttl of %q - background refresh disabled",
				refreshBeforeExpiry,
				ttl,
			)
		}
	}

	negativeTTLNotFound := defaultNegativeTTLNotFound

	if config.NegativeTTLNotFound != "" {
		var err error

		negativeTTLNotFound, err = time.ParseDuration(config.NegativeTTLNotFound)
		if err != nil {
			return nil, fmt.Errorf("invalid cache negative_ttl_not_found value: %w", err)
		}

		if negativeTTLNotFound <= 0 {
			return nil, fmt.Errorf("cache negative_ttl_not_found must be positive, got %v", config.NegativeTTLNotFound)
		}
	}

	negativeTTLTransient := defaultNegativeTTLTransient

	if config.NegativeTTLTransient != "" {
		var err error

		negativeTTLTransient, err = time.ParseDuration(config.NegativeTTLTransient)
		if err != nil {
			return nil, fmt.Errorf("invalid cache negative_ttl_transient value: %w", err)
		}

		if negativeTTLTransient <= 0 {
			return nil, fmt.Errorf("cache negative_ttl_transient must be positive, got %v", config.NegativeTTLTransient)
		}
	}

	c := &cache{
		entries:              make(map[string]*cacheEntry),
		enabled:              config.Enabled,
		ttl:                  ttl,
		refreshBeforeExpiry:  refreshBeforeExpiry,
		negativeTTLNotFound:  negativeTTLNotFound,
		negativeTTLTransient: negativeTTLTransient,
	}

	go c.cleanupLoop(ctx)

	return c, nil
}
