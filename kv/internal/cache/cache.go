package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/config"
)

const (
	defaultNegativeTTLNotFound  = 60 * time.Second
	defaultNegativeTTLTransient = 5 * time.Second
)

// Cache provides TTL-based in-memory caching for secret values.
// It's thread-safe and automatically expires entries based on configured TTL.
type Cache struct {
	entries              map[string]*cacheEntry
	enabled              bool
	ttl                  time.Duration
	refreshBeforeExpiry  time.Duration
	negativeTTLNotFound  time.Duration
	negativeTTLTransient time.Duration
	mu                   sync.RWMutex
}

// cacheEntry holds a Cached value with its expiration time
type cacheEntry struct {
	value     string
	err       error
	expiresAt time.Time
}

// Get retrieves a Cached value by key and returns metadata about Cache state.
//
// Returns:
//   - value: the Cached string value (empty if Cache miss or expired)
//   - found: true if a valid (non-expired) Cache entry exists
//   - needsRefresh: true if entry exists but is within refreshBeforeExpiry window
//   - err: the Cached error from the original fetch operation (nil for successful Cached values)
func (c *Cache) Get(key string) (string, bool, bool, error) {
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

func (c *Cache) Set(key, value string, err error) {
	if !c.enabled {
		return
	}

	// Context errors should NOT be Cached - they indicate caller abandonment, not provider failure
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	ttl, shouldCache := c.selectTTL(err)
	if !shouldCache {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		err:       err,
	}
}

func (c *Cache) selectTTL(err error) (time.Duration, bool) {
	if err == nil {
		return c.ttl, true
	}

	var notFoundErr *kv.KeyNotFoundError
	var transientErr *kv.StoreUnavailableError

	if errors.As(err, &notFoundErr) {
		return c.negativeTTLNotFound, true
	}

	if errors.As(err, &transientErr) {
		return c.negativeTTLTransient, true
	}

	return 0, false
}

func (c *Cache) get(key string) (*cacheEntry, bool, bool) {
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

func (c *Cache) cleanupLoop(ctx context.Context) {
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

func (c *Cache) cleanup() {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.entries {
		if v == nil || now.After(v.expiresAt) || now.Equal(v.expiresAt) {
			delete(c.entries, k)
		}
	}
}

func NewCache(ctx context.Context, config config.CacheConfig) (*Cache, error) {
	if !config.Enabled {
		return &Cache{enabled: false, entries: make(map[string]*cacheEntry)}, nil
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
				"refresh_before_expiry(%v) must be less than ttl(%v) - background refresh disabled",
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

	c := &Cache{
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
