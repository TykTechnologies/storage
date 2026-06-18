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
	"github.com/stretchr/testify/require"
)

func sequencedProvider() *mockProvider {
	var n int32

	return &mockProvider{
		mockGetFunc: func(_ context.Context, _ string) (string, error) {
			return fmt.Sprintf("v%d", atomic.AddInt32(&n, 1)), nil
		},
	}
}

func TestGetWithCacheBypass(t *testing.T) {
	t.Parallel()

	enabledCache := kv.CacheConfig{Enabled: true, TTL: "1m"}

	t.Run("bypass skips cache read and returns fresh value", func(t *testing.T) {
		t.Parallel()

		provider := sequencedProvider()
		store := newTestStore(t, provider, enabledCache)

		first, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", first)

		cached, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", cached)
		require.Equal(t, int32(1), provider.calls.Load(), "second Get must be served from cache")

		fresh, err := store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v2", fresh)
		require.Equal(t, int32(2), provider.calls.Load(), "bypass Get must hit the provider")
	})

	t.Run("bypass re-populates the cache entry", func(t *testing.T) {
		t.Parallel()

		provider := sequencedProvider()
		store := newTestStore(t, provider, enabledCache)

		_, err := store.Get(t.Context(), "db/password") // v1 cached
		require.NoError(t, err)

		fresh, err := store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v2", fresh)

		after, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v2", after)
		require.Equal(t, int32(2), provider.calls.Load(), "post-bypass Get must be served from the re-populated cache")
	})

	t.Run("bypass on a never-cached key behaves like a normal miss", func(t *testing.T) {
		t.Parallel()

		provider := sequencedProvider()
		store := newTestStore(t, provider, enabledCache)

		fresh, err := store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", fresh)
		require.Equal(t, int32(1), provider.calls.Load())

		cached, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", cached)
		require.Equal(t, int32(1), provider.calls.Load(), "value fetched via bypass must be cached")
	})

	t.Run("bypass skips a cached negative entry", func(t *testing.T) {
		t.Parallel()

		var n int32
		provider := &mockProvider{
			mockGetFunc: func(_ context.Context, _ string) (string, error) {
				if atomic.AddInt32(&n, 1) == 1 {
					return "", &kv.KeyNotFoundError{StoreName: "test", KeyPath: "db/password"}
				}

				return "recovered", nil
			},
		}
		store := newTestStore(t, provider, enabledCache)

		_, err := store.Get(t.Context(), "db/password")
		require.Error(t, err)

		_, err = store.Get(t.Context(), "db/password")
		require.Error(t, err)
		require.Equal(t, int32(1), provider.calls.Load(), "error must be served from negative cache")

		fresh, err := store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "recovered", fresh)

		cached, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)
		require.Equal(t, "recovered", cached)
		require.Equal(t, int32(2), provider.calls.Load())
	})

	t.Run("provider error during bypass propagates instead of stale value", func(t *testing.T) {
		t.Parallel()

		var n int32
		provider := &mockProvider{
			mockGetFunc: func(_ context.Context, _ string) (string, error) {
				if atomic.AddInt32(&n, 1) == 1 {
					return "v1", nil
				}

				return "", &kv.StoreUnavailableError{
					StoreName: "test",
					KeyPath:   "db/password",
					Err:       fmt.Errorf("backend down"),
				}
			},
		}
		store := newTestStore(t, provider, enabledCache)

		_, err := store.Get(t.Context(), "db/password")
		require.NoError(t, err)

		_, err = store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.Error(
			t,
			err,
			"bypass asked for a fresh value; a stale fallback would hide rotation failures",
		)

		var unavailable *kv.StoreUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("bypass with disabled cache behaves like a plain Get", func(t *testing.T) {
		t.Parallel()

		provider := sequencedProvider()
		store := newTestStore(t, provider, kv.CacheConfig{Enabled: false})

		got, err := store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", got)

		got, err = store.Get(kv.WithCacheBypass(t.Context()), "db/password")
		require.NoError(t, err)
		require.Equal(t, "v2", got)
	})

	t.Run("context without marker keeps existing cache behavior", func(t *testing.T) {
		t.Parallel()

		provider := sequencedProvider()
		store := newTestStore(t, provider, enabledCache)

		for range 5 {
			got, err := store.Get(t.Context(), "db/password")
			require.NoError(t, err)
			require.Equal(t, "v1", got)
		}

		require.Equal(t, int32(1), provider.calls.Load(), "plain Gets must keep serving from cache")
	})
}

func TestGetWithCacheBypassConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var n int32
		provider := &mockProvider{
			delay: time.Second,
			mockGetFunc: func(_ context.Context, _ string) (string, error) {
				return fmt.Sprintf("v%d", atomic.AddInt32(&n, 1)), nil
			},
		}
		store := newTestStore(t, provider, kv.CacheConfig{Enabled: true, TTL: "1m"})
		ctx := context.Background()

		seeded, err := store.Get(ctx, "db/password")
		require.NoError(t, err)
		require.Equal(t, "v1", seeded)

		var wg sync.WaitGroup

		// Bypass Gets block on the provider's delay; they must dedupe into
		// one fetch via singleflight.
		for range 5 {
			wg.Go(func() {
				got, err := store.Get(kv.WithCacheBypass(ctx), "db/password")
				require.NoError(t, err)
				require.Equal(t, "v2", got)
			})
		}

		// Non-bypass Gets run while the bypass fetch is in flight: they are
		// served the still-valid cached value without blocking.
		for range 5 {
			wg.Go(func() {
				got, err := store.Get(ctx, "db/password")
				require.NoError(t, err)
				require.Equal(t, "v1", got)
			})
		}

		wg.Wait()

		require.Equal(t, int32(2), provider.calls.Load(), "5 concurrent bypass Gets must collapse into one provider fetch")

		after, err := store.Get(ctx, "db/password")
		require.NoError(t, err)
		require.Equal(t, "v2", after)
		require.Equal(t, int32(2), provider.calls.Load())
	})
}
