package kv

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("disable cache", func(t *testing.T) {
		cfg := CacheConfig{Enabled: false}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.False(t, c.enabled)
	})

	t.Run("invalid TTL", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "invalid"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "invalid cache ttl")
	})

	t.Run("negative TTL", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "-5s"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "cache ttl must be positive")
	})

	t.Run("invalid refresh before expiry", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", RefreshBeforeExpiry: "some"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "invalid cache refresh_before_expiry")
	})

	t.Run("negative refresh before expiry", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", RefreshBeforeExpiry: "-1s"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "refresh_before_expiry must be positive")
	})

	t.Run("invalid negative ttl not found", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", NegativeTTLNotFound: "some"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "invalid cache negative_ttl_not_found")
	})

	t.Run("negative value for negative ttl not found", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", NegativeTTLNotFound: "-1s"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "negative_ttl_not_found must be positive")
	})

	t.Run("invalid negative ttl transient", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", NegativeTTLTransient: "some"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "invalid cache negative_ttl_transient")
	})

	t.Run("negative value for negative ttl transient", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", NegativeTTLTransient: "-1s"}
		c, err := newCache(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, c)
		require.Contains(t, err.Error(), "negative_ttl_transient must be positive")
	})

	t.Run("overrides refresh before expiry to zero if its >= ttl", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s", RefreshBeforeExpiry: "1s"}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Empty(t, c.refreshBeforeExpiry)
	})

	t.Run("sets correct defaults", func(t *testing.T) {
		cfg := CacheConfig{Enabled: true, TTL: "1s"}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Empty(t, c.refreshBeforeExpiry)
		require.Equal(t, defaultNegativeTTLNotFound, c.negativeTTLNotFound)
		require.Equal(t, defaultNegativeTTLTransient, c.negativeTTLTransient)
	})

	t.Run("valid config", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := CacheConfig{
			Enabled:              true,
			TTL:                  "100ms",
			RefreshBeforeExpiry:  "50ms",
			NegativeTTLNotFound:  "20s",
			NegativeTTLTransient: "2s",
		}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, 100*time.Millisecond, c.ttl)
		require.Equal(t, 50*time.Millisecond, c.refreshBeforeExpiry)
		require.Equal(t, 20*time.Second, c.negativeTTLNotFound)
		require.Equal(t, 2*time.Second, c.negativeTTLTransient)
		require.NotNil(t, c.entries)
	})
}

func TestCache_GetSet(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defaultConfig := CacheConfig{
		Enabled: true,
		TTL:     "500ms",
	}
	c, err := newCache(ctx, defaultConfig)
	require.NoError(t, err)

	t.Run("cache disabled", func(t *testing.T) {
		cfg := CacheConfig{Enabled: false, TTL: "500ms", RefreshBeforeExpiry: "200ms"}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)

		c.Set("cache-disabled", "some", nil)

		val, exists, needsRefresh, err := c.Get("cache-disabled")
		assert.False(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.NoError(t, err)
	})

	t.Run("cache miss", func(t *testing.T) {
		val, exists, needsRefresh, err := c.Get("non-existent")
		assert.False(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.NoError(t, err)
	})

	t.Run("cache hit", func(t *testing.T) {
		c.Set("key1", "value1", nil)

		val, exists, needsRefresh, err := c.Get("key1")
		assert.True(t, exists)
		assert.False(t, needsRefresh)
		assert.Equal(t, "value1", val)
		assert.NoError(t, err)
	})

	t.Run("negative caching with KeyNotFoundError", func(t *testing.T) {
		expectedErr := &KeyNotFoundError{}
		c.Set("key2", "", expectedErr)

		val, exists, needsRefresh, err := c.Get("key2")
		assert.True(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.Equal(t, expectedErr, err)

		entry, _, _ := c.get("key2")
		expectedTTL := time.Now().Add(defaultNegativeTTLNotFound)
		require.WithinDuration(t, expectedTTL, entry.expiresAt, time.Second)
	})

	t.Run("negative caching with StoreUnavailableError", func(t *testing.T) {
		expectedErr := &StoreUnavailableError{}
		c.Set("key3", "", expectedErr)

		val, exists, needsRefresh, err := c.Get("key3")
		assert.True(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.Equal(t, expectedErr, err)

		entry, _, _ := c.get("key3")
		expectedTTL := time.Now().Add(defaultNegativeTTLTransient)
		require.WithinDuration(t, expectedTTL, entry.expiresAt, time.Second)
	})

	t.Run("negative caching is disabled for context errors", func(t *testing.T) {
		c.Set("key3", "value3", context.Canceled)
		c.Set("key4", "value4", context.DeadlineExceeded)

		val, exists, needsRefresh, err := c.Get("key3")
		assert.False(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.NoError(t, err)

		val, exists, needsRefresh, err = c.Get("key4")
		assert.False(t, exists)
		assert.False(t, needsRefresh)
		assert.Empty(t, val)
		assert.NoError(t, err)

		entry, _, _ := c.get("key3")
		require.Empty(t, entry)

		entry, _, _ = c.get("key4")
		require.Empty(t, entry)
	})
}

func TestCache_RefreshBeforeExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := CacheConfig{
			Enabled:             true,
			TTL:                 "2s",
			RefreshBeforeExpiry: "1s",
		}
		c, err := newCache(t.Context(), cfg)
		require.NoError(t, err)

		c.Set("key1", "value1", nil)
		time.Sleep(time.Second)

		val, exists, needsRefresh, err := c.Get("key1")
		assert.True(t, exists)
		assert.True(t, needsRefresh)
		assert.Equal(t, "value1", val)
		assert.NoError(t, err)
	})
}

func TestCache_CleanupExpiredEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := CacheConfig{
			Enabled:              true,
			TTL:                  "2s",
			NegativeTTLNotFound:  "10s",
			NegativeTTLTransient: "4s",
		}
		c, err := newCache(t.Context(), cfg)
		require.NoError(t, err)

		testEntries := []struct {
			key         string
			value       string
			err         error
			description string
		}{
			{"key1", "value1", nil, "normal entry (2s TTL)"},
			{"key2", "value2", &KeyNotFoundError{}, "not found entry (10s TTL)"},
			{"key3", "value3", &StoreUnavailableError{}, "transient error entry (5s TTL)"},
		}

		for _, entry := range testEntries {
			c.Set(entry.key, entry.value, entry.err)
		}

		// Phase 1: Verify all entries are initially present and accessible
		assertCacheEntry(t, c, "key1", "value1", true, false, "normal entry should be accessible")
		assertCacheEntry(t, c, "key2", "value2", true, true, "not found entry should be accessible with error")
		assertCacheEntry(t, c, "key3", "value3", true, true, "transient error entry should be accessible with error")

		// Phase 2: After 2s - normal entry (key1) should expire, negative entries should remain
		time.Sleep(2 * time.Second)
		synctest.Wait()

		assertCacheEntry(t, c, "key1", "", false, false, "normal entry should be expired")
		assertCacheEntry(t, c, "key2", "value2", true, true, "not found entry should still be present")
		assertCacheEntry(t, c, "key3", "value3", true, true, "transient error entry should still be present")

		_, exists, _ := c.get("key1")
		assert.False(t, exists, "key1 should be removed from internal storage")

		// Phase 3: After 4s total - transient error entry (key3) should expire
		time.Sleep(2 * time.Second)
		synctest.Wait()

		assertCacheEntry(t, c, "key1", "", false, false, "normal entry should still be expired")
		assertCacheEntry(t, c, "key2", "value2", true, true, "not found entry should still be present")
		assertCacheEntry(t, c, "key3", "", false, false, "transient error entry should now be expired")

		_, exists, _ = c.get("key3")
		assert.False(t, exists, "key3 should be removed from internal storage")

		// Phase 4: After 10s total - not found error entry (key2) should expire
		time.Sleep(6 * time.Second)
		synctest.Wait()

		assertCacheEntry(t, c, "key1", "", false, false, "normal entry should still be expired")
		assertCacheEntry(t, c, "key2", "", false, false, "not found entry should now be expired")
		assertCacheEntry(t, c, "key3", "", false, false, "transient error entry should still be expired")

		_, exists, _ = c.get("key2")
		assert.False(t, exists, "key2 should be removed from internal storage")
	})
}

func TestCache_CleanupStopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		cfg := CacheConfig{
			Enabled: true,
			TTL:     "1s",
		}
		c, err := newCache(ctx, cfg)
		require.NoError(t, err)

		c.Set("key1", "value1", nil)

		assertCacheEntry(t, c, "key1", "value1", true, false, "Should exist")

		cancel()
		synctest.Wait()

		time.Sleep(2 * time.Second)
		synctest.Wait()

		_, exists, _, err := c.Get("key1")
		require.NoError(t, err)
		assert.False(t, exists, "Get should return false due to lazy eviction (expired)")

		_, physicallyInMap, _ := c.get("key1")
		assert.True(
			t,
			physicallyInMap,
			"The entry should still physically exist in the map because the background cleanup is stopped",
		)
	})
}

func TestCache_Concurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := CacheConfig{Enabled: true, TTL: "10m"}
	c, err := newCache(ctx, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for j := range iterations {
				key := fmt.Sprintf("key-%d", j)
				c.Set(key, "value", nil)
			}
		}()
	}

	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for j := range iterations {
				key := fmt.Sprintf("key-%d", j)
				_, _, _, err := c.Get(key)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	c.mu.RLock()
	assert.Greater(t, len(c.entries), 0)
	c.mu.RUnlock()
}

func assertCacheEntry(
	t *testing.T,
	c *cache,
	key,
	expectedValue string,
	shouldExist,
	shouldHaveError bool,
	description string,
) {
	t.Helper()

	val, exists, _, err := c.Get(key)

	if shouldExist {
		assert.True(t, exists, "%s: key %s should exist in cache", description, key)
		assert.Equal(t, expectedValue, val, "%s: key %s should have expected value", description, key)
	} else {
		assert.False(t, exists, "%s: key %s should not exist in cache", description, key)
	}

	if shouldHaveError {
		assert.Error(t, err, "%s: key %s should return an error", description, key)
	} else {
		assert.NoError(t, err, "%s: key %s should not return an error", description, key)
	}
}
