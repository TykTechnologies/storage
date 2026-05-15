package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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
	ctx := context.Background()

	t.Run("nil provider", func(t *testing.T) {
		store, err := NewSecretStore(ctx, "test", nil, CacheConfig{Enabled: true, TTL: "1m"})
		require.Error(t, err)
		require.Nil(t, store)
		require.Contains(t, err.Error(), "provider cannot be nil")
	})

	t.Run("invalid cache config", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(ctx, "test", provider, CacheConfig{
			Enabled: true,
			TTL:     "invalid-duration",
		})
		require.Error(t, err)
		require.Nil(t, store)
		require.Contains(t, err.Error(), "failed to create secret store")
	})

	t.Run("negative TTL", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(ctx, "test", provider, CacheConfig{
			Enabled: true,
			TTL:     "-10s",
		})
		require.Error(t, err)
		require.Nil(t, store)
	})

	t.Run("cache disabled", func(t *testing.T) {
		provider := &mockProvider{}
		store, err := NewSecretStore(ctx, "test", provider, CacheConfig{
			Enabled: false,
		})
		require.NoError(t, err)
		require.NotNil(t, store)

		// Every call should hit provider
		store.GetSecret(ctx, "key1")
		store.GetSecret(ctx, "key1")
		require.Equal(t, int32(2), provider.calls.Load())
	})
}

func TestGetSecret_CacheMissAndHit(t *testing.T) {
	ctx := context.Background()
	provider := &mockProvider{}
	cfg := CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	// First call: cache miss
	val, err := store.GetSecret(ctx, "secret-1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cache miss should call provider")

	// Second call: cache hit
	val, err = store.GetSecret(ctx, "secret-1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cache hit should not call provider")
}

func TestGetSecret_ProviderErrorReturned(t *testing.T) {
	ctx := context.Background()
	expectedErr := &KeyNotFoundError{}
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			return "", expectedErr
		},
	}
	cfg := CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.GetSecret(ctx, "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), provider.calls.Load())
}

func TestGetSecret_NegativeCachingForKeyNotFoundError(t *testing.T) {
	ctx := context.Background()
	expectedErr := &KeyNotFoundError{}
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			return "secret", expectedErr
		},
	}
	cfg := CacheConfig{
		Enabled:             true,
		TTL:                 "1m",
		NegativeTTLNotFound: "30s",
	}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.GetSecret(ctx, "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val, "value should be empty even if provider returned non-empty string")
	assert.Equal(t, int32(1), provider.calls.Load())

	val, err = store.GetSecret(ctx, "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), provider.calls.Load(), "cached error should prevent provider call")
}

func TestGetSecret_SingleFlightDeduplication(t *testing.T) {
	ctx := context.Background()
	provider := &mockProvider{
		delay: 50 * time.Millisecond,
	}
	cfg := CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const workers = 100
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			val, err := store.GetSecret(ctx, "concurrent-secret")
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

func TestGetSecret_CacheDisabled_AlwaysCallsProvider(t *testing.T) {
	ctx := context.Background()
	provider := &mockProvider{}
	cfg := CacheConfig{Enabled: false}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	val, err := store.GetSecret(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(t, int32(1), provider.calls.Load())

	val, err = store.GetSecret(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "mock-secret", val)
	assert.Equal(
		t,
		int32(2),
		provider.calls.Load(),
		"cache disabled should call provider every time",
	)
}

func TestGetSecret_DifferentKeysIndependent(t *testing.T) {
	ctx := context.Background()
	var callCount atomic.Int32
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			callCount.Add(1)
			return fmt.Sprintf("secret-%s", path), nil
		},
	}
	cfg := CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	val1, err := store.GetSecret(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "secret-key1", val1)

	val2, err := store.GetSecret(ctx, "key2")
	require.NoError(t, err)
	assert.Equal(t, "secret-key2", val2)

	assert.Equal(t, int32(2), callCount.Load(), "different keys should trigger separate provider calls")

	val1, err = store.GetSecret(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "secret-key1", val1)
	assert.Equal(t, int32(2), callCount.Load(), "refetch should use cache")
}

func TestGetSecret_ProviderTimeoutEnforced(t *testing.T) {
	ctx := context.Background()
	provider := &mockProvider{
		delay: 10 * time.Second,
	}
	cfg := CacheConfig{Enabled: true, TTL: "1m"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	start := time.Now()
	val, err := store.GetSecret(ctx, "slow-key")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout fetching")
	assert.Empty(t, val)
	assert.Less(t, elapsed, 6*time.Second, "should timeout at defaultProviderTimeout (5s)")
}

func TestStaleWhileRevalidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var callCount int32

		provider := &mockProvider{
			mockGetFunc: func(ctx context.Context, path string) (string, error) {
				count := atomic.AddInt32(&callCount, 1)
				return fmt.Sprintf("secret-v%d", count), nil
			},
		}
		cfg := CacheConfig{Enabled: true, TTL: "5s", RefreshBeforeExpiry: "1s"}
		store, err := NewSecretStore(t.Context(), "test-store", provider, cfg)
		require.NoError(t, err)

		// Cache miss
		val, err := store.GetSecret(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v1", val)

		time.Sleep(4 * time.Second)
		synctest.Wait()

		// Cache hit and triggers background refresh
		val, err = store.GetSecret(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v1", val)

		// Wait for background refresh to finish
		synctest.Wait()

		// Refreshed value
		start := time.Now()
		val, err = store.GetSecret(t.Context(), "stale-secret")
		require.NoError(t, err)
		assert.Equal(t, "secret-v2", val)
		assert.Equal(t, int32(2), callCount)

		latency := time.Since(start)
		require.Less(t, latency, 10*time.Millisecond, "should return stale value immediately")
	})
}

func TestBackgroundRefreshDeduplication(t *testing.T) {
	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	store, err := NewSecretStore(context.Background(), "test", provider, CacheConfig{
		Enabled:             true,
		TTL:                 "1s",
		RefreshBeforeExpiry: "500ms",
	})
	require.NoError(t, err)

	_, err = store.GetSecret(context.Background(), "key1")
	require.NoError(t, err)

	time.Sleep(600 * time.Millisecond)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := store.GetSecret(context.Background(), "key1")
			require.NoError(t, err)
		}()
	}

	// We wanna be sure that second request to provider is finished
	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	require.Equal(t, int32(2), provider.calls.Load())
}

func TestBackgroundRefreshSurvivesRequestCancellation(t *testing.T) {
	var callCount atomic.Int32
	provider := &mockProvider{
		delay: 100 * time.Millisecond,
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			count := callCount.Add(1)
			return fmt.Sprintf("secret-v%d", count), nil
		},
	}

	store, err := NewSecretStore(context.Background(), "test", provider, CacheConfig{
		Enabled:             true,
		TTL:                 "2s",
		RefreshBeforeExpiry: "1s",
	})
	require.NoError(t, err)

	// Initial fetch
	val, err := store.GetSecret(context.Background(), "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v1", val)

	time.Sleep(time.Second)

	cancelCtx, cancel := context.WithCancel(context.Background())
	val, err = store.GetSecret(cancelCtx, "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v1", val)

	cancel()

	// Wait for background refresh to complete
	time.Sleep(200 * time.Millisecond)

	// Verify fresh value was cached despite cancellation
	val, err = store.GetSecret(context.Background(), "key1")
	require.NoError(t, err)
	require.Equal(t, "secret-v2", val)
	require.Equal(t, int32(2), callCount.Load())
}

func TestConcurrentBackgroundRefreshDifferentKeys(t *testing.T) {
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

	store, err := NewSecretStore(context.Background(), "test", provider, CacheConfig{
		Enabled:             true,
		TTL:                 "2s",
		RefreshBeforeExpiry: "1s",
	})
	require.NoError(t, err)

	_, err = store.GetSecret(context.Background(), "key1")
	require.NoError(t, err)
	_, err = store.GetSecret(context.Background(), "key2")
	require.NoError(t, err)

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		store.GetSecret(context.Background(), "key1")
	}()

	go func() {
		defer wg.Done()
		store.GetSecret(context.Background(), "key2")
	}()

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for refreshes to complete

	// Each key should have exactly 2 calls (initial + 1 refresh)
	require.Equal(t, int32(2), key1Calls.Load())
	require.Equal(t, int32(2), key2Calls.Load())
}

func TestContextCancellationDoesNotPoisonCache(t *testing.T) {
	ctx := context.Background()

	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	cfg := CacheConfig{Enabled: true, TTL: "5s"}
	store, err := NewSecretStore(ctx, "test-store", provider, cfg)
	require.NoError(t, err)

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	// Foreground fetch
	val, err := store.GetSecret(cancelCtx, "cancel-secret")
	require.Error(t, err)
	require.Contains(t, err.Error(), "request cancelled while fetching")
	require.Empty(t, val)

	require.Equal(t, int32(1), provider.calls.Load())

	val, err = store.GetSecret(ctx, "cancel-secret")
	require.NoError(t, err)
	require.Equal(t, "mock-secret", val)

	require.Equal(t, int32(2), provider.calls.Load())

	val, err = store.GetSecret(ctx, "cancel-secret")
	require.NoError(t, err)
	require.Equal(t, "mock-secret", val)

	require.Equal(t, int32(2), provider.calls.Load())
}
