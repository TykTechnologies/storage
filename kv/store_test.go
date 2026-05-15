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

// TODO: Add test case with testing NewSecretStore edge-cases and errors

func TestSecretStore_GetSecret(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("successful fetch and cache hit", func(t *testing.T) {
		provider := &mockProvider{}
		cfg := CacheConfig{Enabled: true, TTL: "1m"}
		store, err := NewSecretStore(ctx, "test-store", provider, cfg)
		require.NoError(t, err)

		// Cache miss
		val, err := store.GetSecret(ctx, "secret-1")
		require.NoError(t, err)
		assert.Equal(t, "mock-secret", val)
		assert.Equal(t, int32(1), provider.calls.Load())

		// Cache hit
		val, err = store.GetSecret(ctx, "secret-1")
		require.NoError(t, err)
		assert.Equal(t, "mock-secret", val)
		assert.Equal(t, int32(1), provider.calls.Load())
	})

	t.Run("provider error", func(t *testing.T) {
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
		require.ErrorAs(t, err, &expectedErr)
		assert.Empty(t, val)
		assert.Equal(t, int32(1), provider.calls.Load())
	})

	t.Run("ignored cached value when it has an error", func(t *testing.T) {
		expectedErr := &KeyNotFoundError{}
		provider := &mockProvider{
			mockGetFunc: func(ctx context.Context, path string) (string, error) {
				return "secret", expectedErr
			},
		}
		cfg := CacheConfig{Enabled: true, TTL: "1m"}
		store, err := NewSecretStore(ctx, "test-store", provider, cfg)
		require.NoError(t, err)

		val, err := store.GetSecret(ctx, "secret-err")
		require.ErrorAs(t, err, &expectedErr)
		assert.Empty(t, val)

		val, err = store.GetSecret(ctx, "secret-err")
		require.ErrorAs(t, err, &expectedErr)
		assert.Empty(t, val)

		assert.Equal(t, int32(1), provider.calls.Load())
	})

	t.Run("single-flight deduplication", func(t *testing.T) {
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

		// Provider should only be called once despite 100 requests
		assert.Equal(t, int32(1), provider.calls.Load())
	})
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

// We can't use synctest here because background goroutine is using its own
// context which can't be controlled by synctest. Potentially this test can be
// flaky because it depends on time.Sleep().
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
