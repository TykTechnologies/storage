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
	stop    chan struct{}
}

func (c *cache) Get(key string) (string, bool) {
	if !c.enabled {
		return "", false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		return "", false
	}

	if time.Now().After(entry.expiresAt) {
		return "", false
	}

	return entry.value, true
}

func (c *cache) Set(key, value string) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *cache) cleanupLoop() {
	// FIX: Ask Kofo's opinion about duration
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()

			for k, v := range c.entries {
				if v == nil || now.After(v.expiresAt) {
					delete(c.entries, k)
				}
			}

			c.mu.Unlock()
		case <-c.stop:
			return
		}
	}
}

func (c *cache) Close() {
	if c.enabled {
		close(c.stop)
	}
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

	c := &cache{
		entries: make(map[string]*cacheEntry),
		enabled: true,
		ttl:     ttl,
		stop:    make(chan struct{}),
	}

	go c.cleanupLoop()

	return c, nil
}
