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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProvider struct {
	calls       atomic.Int32
	delay       time.Duration
	mockGetFunc func(ctx context.Context, path string) (string, error)
	closed      atomic.Bool
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

func (m *mockProvider) Close(_ context.Context) error {
	m.closed.Store(true)
	return nil
}

func TestNewSecretStore(t *testing.T) {
	t.Parallel()

	t.Run("nil provider", func(t *testing.T) {
		t.Parallel()

		store, err := NewSecretStore("test", nil)
		require.Error(t, err)
		require.Nil(t, store)
		require.Contains(t, err.Error(), "provider cannot be nil")
	})

	t.Run("assigns default timeout", func(t *testing.T) {
		t.Parallel()

		store, err := NewSecretStore("test", &mockProvider{})
		require.NoError(t, err)
		require.NotNil(t, store)
		require.Equal(t, kv.DefaultOperationTimeout, store.timeout)
	})
}

func TestGet_AlwaysCallsProvider(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{}
	store := newTestStore(t, provider)

	for i := 1; i <= 3; i++ {
		val, err := store.Get(t.Context(), "key1")
		require.NoError(t, err)
		assert.Equal(t, "mock-secret", val)
		assert.Equal(t, int32(i), provider.calls.Load(),
			"every sequential Get must reach the provider")
	}
}

func TestGet_ProviderErrorReturned(t *testing.T) {
	t.Parallel()

	expectedErr := &kv.KeyNotFoundError{}
	provider := &mockProvider{
		mockGetFunc: func(ctx context.Context, path string) (string, error) {
			return "leaked-secret", expectedErr
		},
	}
	store := newTestStore(t, provider)

	val, err := store.Get(t.Context(), "secret-err")
	require.Error(t, err)
	require.ErrorAs(t, err, &expectedErr, "provider error must stay unwrappable")
	assert.Empty(t, val, "value must be empty even if the provider returned one alongside the error")
	assert.Equal(t, int32(1), provider.calls.Load())

	_, err = store.Get(t.Context(), "secret-err")
	require.Error(t, err)
	assert.Equal(t, int32(2), provider.calls.Load(), "errors must not be stored")
}

func TestGet_SingleFlightDeduplication(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		provider := &mockProvider{
			delay: time.Second,
		}
		store := newTestStore(t, provider)

		var wg sync.WaitGroup

		start := time.Now()

		for range 100 {
			wg.Go(func() {
				val, err := store.Get(t.Context(), "concurrent-secret")
				require.NoError(t, err)
				assert.Equal(t, "mock-secret", val)
			})
		}

		wg.Wait()

		require.Less(
			t,
			time.Since(start),
			1001*time.Millisecond,
			"all 100 requests will return after first success singleflight call",
		)
		assert.Equal(
			t,
			int32(1),
			provider.calls.Load(),
			"100 concurrent requests should deduplicate to 1 provider call",
		)
	})
}

func TestGet_SingleFlightIsPerKey(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		provider := &mockProvider{
			delay: time.Second,
			mockGetFunc: func(ctx context.Context, path string) (string, error) {
				return fmt.Sprintf("secret-%s", path), nil
			},
		}
		store := newTestStore(t, provider)

		var (
			mu      sync.Mutex
			results = map[string]string{}
			wg      sync.WaitGroup
		)

		for _, key := range []string{"key1", "key2", "key3"} {
			wg.Go(func() {
				val, err := store.Get(t.Context(), key)
				require.NoError(t, err)

				mu.Lock()
				defer mu.Unlock()

				results[key] = val
			})
		}

		wg.Wait()

		assert.Equal(t, map[string]string{
			"key1": "secret-key1",
			"key2": "secret-key2",
			"key3": "secret-key3",
		}, results, "each key must receive its own value")
		assert.Equal(t, int32(3), provider.calls.Load(),
			"distinct keys must not share a singleflight slot")
	})
}

func TestGet_TimeoutEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		opts            []Option
		expectedTimeout time.Duration
	}{
		{
			name:            "Use default timeout if not explicitly provided",
			opts:            nil,
			expectedTimeout: 5 * time.Second,
		},
		{
			name:            "Override default timeout with custom duration",
			opts:            []Option{WithTimeout(10 * time.Second)},
			expectedTimeout: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				provider := &mockProvider{
					delay: 30 * time.Second,
				}

				store, err := NewSecretStore("test-store", provider, tt.opts...)
				require.NotNil(t, store)
				require.NoError(t, err)
				t.Cleanup(func() {
					store.Close(t.Context())
				})

				start := time.Now()

				var wg sync.WaitGroup
				wg.Go(func() {
					val, err := store.Get(t.Context(), "slow-key")
					require.Error(t, err)
					require.Contains(t, err.Error(), "timeout fetching",
						"timeout error must name the path that timed out")
					assert.Empty(t, val)
				})

				synctest.Wait()
				wg.Wait()

				elapsed := time.Since(start)
				assert.Equal(t, tt.expectedTimeout, elapsed)
			})
		})
	}
}

func TestGet_CallerCancellationDoesNotAbortSharedFetch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		provider := &mockProvider{
			delay: time.Second,
		}
		store := newTestStore(t, provider)

		var wg sync.WaitGroup

		leavingCtx, cancel := context.WithCancel(context.Background())

		wg.Go(func() {
			_, err := store.Get(leavingCtx, "shared-key")
			require.ErrorIs(t, err, context.Canceled)
		})

		// Block until that caller is parked inside the provider call.
		synctest.Wait()

		// Now a second caller attaches to the same in-flight fetch, and stays.
		var (
			stayingVal string
			stayingErr error
		)
		wg.Go(func() {
			stayingVal, stayingErr = store.Get(t.Context(), "shared-key")
		})

		synctest.Wait()

		// Drop the caller that owns the fetch context.
		cancel()

		wg.Wait()

		require.NoError(t, stayingErr, "remaining caller must not inherit the cancellation")
		assert.Equal(t, "mock-secret", stayingVal)
		assert.Equal(t, int32(1), provider.calls.Load())
	})
}

func TestClose_LifecycleBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("Get rejects calls immediately after close", func(t *testing.T) {
		t.Parallel()

		provider := &mockProvider{}
		store := newTestStore(t, provider)

		err := store.Close(t.Context())
		require.NoError(t, err)

		val, err := store.Get(t.Context(), "any-key")
		assert.ErrorIs(t, err, kv.ErrStoreClosed)
		assert.Empty(t, val)
		assert.Equal(t, int32(0), provider.calls.Load(), "Should never hit provider once closed")
	})

	t.Run("close during an in-flight fetch settles cleanly", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			provider := &mockProvider{
				delay: 2 * time.Second,
			}
			store := newTestStore(t, provider)

			var wg sync.WaitGroup
			wg.Go(func() {
				// The fetch was admitted before the close, so it is allowed to
				// finish and serve its caller.
				_, err := store.Get(t.Context(), "mid-flight-key")
				require.NoError(t, err)
			})

			// Let the goroutine reach the provider call.
			time.Sleep(100 * time.Millisecond)

			err := store.Close(t.Context())
			require.NoError(t, err)

			time.Sleep(2 * time.Second)
			synctest.Wait()
			wg.Wait()

			// Anything arriving after the close is rejected outright.
			_, err = store.Get(t.Context(), "mid-flight-key")
			assert.ErrorIs(t, err, kv.ErrStoreClosed)
		})
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		t.Parallel()

		provider := &mockProvider{}
		store := newTestStore(t, provider)

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				err := store.Close(t.Context())
				require.NoError(t, err)
			})
		}

		wg.Wait()

		assert.True(t, provider.closed.Load(), "Close must delegate to the provider")
	})
}

func newTestStore(t *testing.T, provider kv.Provider) *SecretStore {
	t.Helper()

	store, err := NewSecretStore("test", provider)
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close(t.Context())
	})

	return store
}
