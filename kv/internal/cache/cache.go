package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TykTechnologies/storage/kv"
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
	interval := min(c.ttl, c.negativeTTLNotFound, c.negativeTTLTransient)
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
	var expired []string

	c.mu.RLock()
	for k, v := range c.entries {
		if v == nil || now.After(v.expiresAt) || now.Equal(v.expiresAt) {
			expired = append(expired, k)
		}
	}
	c.mu.RUnlock()

	if len(expired) == 0 {
		return
	}

	c.mu.Lock()
	for _, k := range expired {
		entry := c.entries[k]
		if entry == nil || now.After(entry.expiresAt) || now.Equal(entry.expiresAt) {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}

func NewCache(ctx context.Context, config kv.CacheConfig) (*Cache, error) {
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

	refreshBeforeExpiry, err := parseRefreshBeforeExpiry(config.RefreshBeforeExpiry, ttl)
	if err != nil {
		return nil, err
	}

	negativeTTLNotFound, err := parseOptionalDuration(
		config.NegativeTTLNotFound,
		defaultNegativeTTLNotFound,
		"negative_ttl_not_found",
	)
	if err != nil {
		return nil, err
	}

	negativeTTLTransient, err := parseOptionalDuration(
		config.NegativeTTLTransient,
		defaultNegativeTTLTransient,
		"negative_ttl_transient",
	)
	if err != nil {
		return nil, err
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

// parseOptionalDuration parses a duration string, returning a default value if empty.
// It also validates that the parsed duration is strictly positive.
func parseOptionalDuration(val string, defaultVal time.Duration, name string) (time.Duration, error) {
	if val == "" {
		return defaultVal, nil
	}

	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid cache %s value: %w", name, err)
	}

	if d <= 0 {
		return 0, fmt.Errorf("cache %s must be positive, got %v", name, val)
	}

	return d, nil
}

// parseRefreshBeforeExpiry parses and validates the refresh_before_expiry configuration.
func parseRefreshBeforeExpiry(val string, ttl time.Duration) (time.Duration, error) {
	if val == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid cache refresh_before_expiry value: %w", err)
	}

	if d < 0 {
		return 0, fmt.Errorf("cache refresh_before_expiry must be positive, got %v", val)
	}

	if d >= ttl {
		return 0, fmt.Errorf(
			"refresh_before_expiry(%v) must be less than ttl(%v)",
			d,
			ttl,
		)
	}

	return d, nil
}
