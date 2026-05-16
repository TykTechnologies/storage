package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProvider struct {
	calls       atomic.Int32
	delay       time.Duration
	mockGetFunc func(ctx context.Context, path string) (string, error)
}

func (m *mockProvider) Get(ctx context.Context, path string) (string, error) {
	m.calls.Add(1)

	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	if m.mockGetFunc != nil {
		return m.mockGetFunc(ctx, path)
	}

	return "mock-secret", nil
}

func TestNewSecretStore(t *testing.T) {
	t.Parallel()

	t.Run("nil provider", func(t *testing.T) {
		store, err := NewSecretStore(t.Context(), "test", nil, config.CacheConfig{Enabled: true, TTL: "1m"})
		require.Error(t, err)
		require.Nil(t, store)
		require.Contains(t, err.Error(), "provider cannot be nil")
	})

	t.Run("invalid cache config", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
			Enabled: true,
			TTL:     "invalid-duration",
		})
		require.Error(t, err)
		require.Nil(t, store)
		require.Contains(t, err.Error(), "failed to create secret store")
	})

	t.Run("negative TTL", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
			Enabled: true,
			TTL:     "-10s",
		})
		require.Error(t, err)
		require.Nil(t, store)
	})

	t.Run("cache disabled", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
			Enabled: false,
		})
		require.NoError(t, err)
		require.NotNil(t, store)

		// Every call should hit provider
		_, err = store.Get(t.Context(), "key1")
		require.NoError(t, err)
		_, err = store.Get(t.Context(), "key1")
		require.NoError(t, err)
		require.Equal(t, int32(2), provider.calls.Load())
	})
}

func TestGet_CacheMissAndHit(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{}
	cfg := config.CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	// First call: cache miss
	val, err := store.Get(t.Context(), "secret-1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cache miss should call provider")

	// Second call: cache hit
	val, err = store.Get(t.Context(), "secret-1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cache hit should not call provider")
}

func TestGet_ProviderErrorReturned(t *testing.T) {
	t.Parallel()

	expectedErr := &kv.KeyNotFoundError{}
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			return "", expectedErr
		},
	}
	cfg := config.CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.Get(t.Context(), "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), provider.calls.Load())
}

func TestGet_NegativeCachingForKeyNotFoundError(t *testing.T) {
	t.Parallel()

	expectedErr := &kv.KeyNotFoundError{}
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			return "secret", expectedErr
		},
	}
	cfg := config.CacheConfig{
		Enabled:             true,
		TTL:                 "1m",
		NegativeTTLNotFound: "30s",
	}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.Get(t.Context(), "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val, "value should be empty even if provider returned non-empty string")
	assert.Equal(t, int32(1), provider.calls.Load())

	val, err = store.Get(t.Context(), "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cached error should prevent provider call")
}

func TestGet_SingleFlightDeduplication(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{
		delay: 50 * time.Millisecond,
	}
	cfg := config.CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const workers = 100
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			val, err := store.Get(t.Context(), "concurrent-secret")
			require.NoError(t, err)
			assert.Equal(t, "mock-secret", val)
		}()
	}

	wg.Wait()

	assert.Equal(
		t,
		int32(1),
		provider.calls.Load(),
		"100 concurrent requests should deduplicate to 1 provider call",
	)
}

func TestGet_CacheDisabled_AlwaysCallsProvider(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{}
	cfg := config.CacheConfig{Enabled: false}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.Get(t.Context(), "key1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load())

	val, err = store.Get(t.Context(), "key1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(
		t,
		int32(2),
		provider.calls.Load(),
		"cache disabled should call provider every time",
	)
}

func TestGet_DifferentKeysIndependent(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			callCount.Add(1)
			return fmt.Sprintf("secret-%s", path), nil
		},
	}
	cfg := config.CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	val1, err := store.Get(t.Context(), "key1")
	require.NoError(t, err)
	assert.Equal(t, "secret-key1", val1)

	val2, err := store.Get(t.Context(), "key2")
	require.NoError(t, err)
	assert.Equal(t, "secret-key2", val2)

	assert.Equal(t, int32(2), callCount.Load(), "different keys should trigger separate provider calls")

	val1, err = store.Get(t.Context(), "key1")
	require.NoError(t, err)
	assert.Equal(t, "secret-key1", val1)
	assert.Equal(t, int32(2), callCount.Load(), "refetch should use cache")
}

func TestGet_ProviderTimeoutEnforced(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{
		delay: 10 * time.Second,
	}
	cfg := config.CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	start := time.Now()
	val, err := store.Get(t.Context(), "slow-key")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout fetching")
	assert.Empty(t, val)
	assert.Less(t, elapsed, 6*time.Second, "should timeout at defaultProviderTimeout (5s)")
}

func TestStaleWhileRevalidate(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var callCount int32

		provider := &mockProvider{
			mockGetFunc: func(ctx context.Context, path string) (string, error) {
				count := atomic.AddInt32(&callCount, 1)
				return fmt.Sprintf("secret-v%d", count), nil
			},
		}
		cfg := config.CacheConfig{Enabled: true, TTL: "5s", RefreshBeforeExpiry: "1s"}
		store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
		require.NoError(t, err)

		// Cache miss
		val, err := store.Get(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v1", val)

		time.Sleep(4 * time.Second)
		synctest.Wait()

		// Cache hit and triggers background refresh
		val, err = store.Get(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v1", val)

		// Wait for background refresh to finish
		synctest.Wait()

		// Refreshed value
		start := time.Now()
		val, err = store.Get(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v2", val)
		assert.Equal(t, int32(2), callCount)

		latency := time.Since(start)
		require.Less(t, latency, 10*time.Millisecond, "should return stale value immediately")
	})
}

func TestBackgroundRefreshDeduplication(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
		Enabled:             true,
		TTL:                 "1s",
		RefreshBeforeExpiry: "500ms",
	})
	require.NoError(t, err)

	_, err = store.Get(t.Context(), "key1")
	require.NoError(t, err)

	time.Sleep(600 * time.Millisecond)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := store.Get(context.Background(), "key1")
			require.NoError(t, err)
		}()
	}

	// We wanna be sure that second request to provider is finished
	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	require.Equal(t, int32(2), provider.calls.Load())
}

func TestBackgroundRefreshSurvivesRequestCancellation(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	provider := &mockProvider{
		delay: 100 * time.Millisecond,
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			count := callCount.Add(1)
			return fmt.Sprintf("secret-v%d", count), nil
		},
	}

	store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
		Enabled:             true,
		TTL:                 "2s",
		RefreshBeforeExpiry: "1s",
	})
	require.NoError(t, err)

	// Initial fetch
	val, err := store.Get(t.Context(), "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v1", val)

	time.Sleep(time.Second)

	cancelCtx, cancel := context.WithCancel(context.Background())
	val, err = store.Get(cancelCtx, "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v1", val)

	cancel()

	// Wait for background refresh to complete
	time.Sleep(200 * time.Millisecond)

	// Verify fresh value was cached despite cancellation
	val, err = store.Get(t.Context(), "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v2", val)
	require.Equal(t, int32(2), callCount.Load())
}

func TestConcurrentBackgroundRefreshDifferentKeys(t *testing.T) {
	t.Parallel()

	var key1Calls, key2Calls atomic.Int32
	provider := &mockProvider{
		delay: 100 * time.Millisecond,
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			if path == "key1" {
				key1Calls.Add(1)
				return "secret-key1", nil
			}

			key2Calls.Add(1)
			return "secret-key2", nil
		},
	}

	store, err := NewSecretStore(t.Context(), "test", provider, config.CacheConfig{
		Enabled:             true,
		TTL:                 "2s",
		RefreshBeforeExpiry: "1s",
	})
	require.NoError(t, err)

	_, err = store.Get(t.Context(), "key1")
	require.NoError(t, err)
	_, err = store.Get(t.Context(), "key2")
	require.NoError(t, err)

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		_, err := store.Get(t.Context(), "key1")
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()

		_, err := store.Get(t.Context(), "key2")
		require.NoError(t, err)
	}()

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for refreshes to complete

	// Each key should have exactly 2 calls (initial + 1 refresh)
	require.Equal(t, int32(2), key1Calls.Load())
	require.Equal(t, int32(2), key2Calls.Load())
}

func TestContextCancellationDoesNotPoisonCache(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	cfg := config.CacheConfig{Enabled: true, TTL: "5s"}
	store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
	require.NoError(t, err)

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	// Foreground fetch
	val, err := store.Get(cancelCtx, "cancel-secret")
	require.Error(t, err)
	require.Contains(t, err.Error(), "request cancelled while fetching")
	require.Empty(t, val)

	require.Equal(t, int32(1), provider.calls.Load())

	val, err = store.Get(t.Context(), "cancel-secret")
	require.NoError(t, err)
	require.Equal(t, "mock-secret", val)

	require.Equal(t, int32(2), provider.calls.Load())

	val, err = store.Get(t.Context(), "cancel-secret")
	require.NoError(t, err)
	require.Equal(t, "mock-secret", val)

	require.Equal(t, int32(2), provider.calls.Load())
}

func TestUnwrapReturnsStoredProvider(t *testing.T) {
	provider := &mockProvider{}

	store, err := NewSecretStore(t.Context(), "test-store", provider, config.CacheConfig{
		Enabled: false,
	})
	require.NotNil(t, store)
	require.NoError(t, err)

	p := store.Unwrap()
	require.Equal(t, provider, p)
}
