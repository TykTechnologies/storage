package registry

import (
	"context"
	"encoding/json"
	"errors"
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
	initFunc     func(ctx context.Context) error
	closeFunc    func(ctx context.Context) error
	isStandalone bool
	calls        atomic.Int32
}

func (m *mockProvider) Get(ctx context.Context, key string) (string, error) {
	m.calls.Add(1)
	return "value", nil
}

func (m *mockProvider) Init(ctx context.Context) error {
	if m.initFunc != nil {
		return m.initFunc(ctx)
	}

	return nil
}

func (m *mockProvider) Close(ctx context.Context) error {
	if m.closeFunc != nil {
		return m.closeFunc(ctx)
	}

	return nil
}

func (m *mockProvider) IsStandalone() bool {
	return m.isStandalone
}

type mockLogger struct {
	warnCalls int
}

func (l *mockLogger) Warn(_ string, _ map[string]any) {
	l.warnCalls++
}
func (*mockLogger) Warnf(_ string, _ ...any) {}

func newFactory(initFunc, closeFunc func(ctx context.Context) error) kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		return &mockProvider{
			initFunc:  initFunc,
			closeFunc: closeFunc,
		}, nil
	}
}

func TestNewRegistry(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	require.NotNil(t, registry)
	require.NotNil(t, registry.stores)
	require.NotNil(t, registry.factories)
}

// TODO: Update the test case when providers are set, to assert
// that all OSS providers are registered.
func TestNewDefaultRegistry(t *testing.T) {}

func TestAddFactory(t *testing.T) {
	t.Parallel()

	t.Run("successful registration", func(t *testing.T) {
		r := NewRegistry()

		err := r.Add(kv.Env, newFactory(nil, nil))
		require.NoError(t, err)

		err = r.Add(kv.Inline, newFactory(nil, nil))
		require.NoError(t, err)

		require.Len(t, r.factories, 2)
	})

	t.Run("reject empty provider type", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be empty")

		require.Len(t, r.factories, 0)
	})

	t.Run("prevent factory duplication override", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("env", newFactory(nil, nil))
		require.NoError(t, err)

		err = r.Add("env", newFactory(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already provided")

		require.Len(t, r.factories, 1)
	})

	t.Run("reject nil factory", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("env", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "factory cannot be nil")
		require.Len(t, r.factories, 0)
	})
}

func TestGetStore(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	err := r.Add("valid", newFactory(nil, nil))
	require.NoError(t, err)

	err = r.InitStores(t.Context(), &kv.Config{
		Stores: map[string]kv.StoreConfig{
			"valid-1": {Type: "valid", Required: true},
		},
	})
	require.NoError(t, err)

	p, err := r.GetStore("valid-1")
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestInitStores_BlastRadius(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		storeType   kv.ProviderType
		required    bool
		factoryErr  error
		initErr     error
		expectError bool
	}

	table := []testCase{
		{
			name:        "required store initializes perfectly",
			storeType:   kv.Env,
			required:    true,
			expectError: false,
		},
		{
			name:        "unregistered provider type fails if required",
			storeType:   kv.Inline,
			required:    true,
			expectError: true,
		},
		{
			name:        "unregistered provider type skipped if optional",
			storeType:   kv.Inline,
			required:    false,
			expectError: false,
		},
		{
			name:        "factory generation failure blocks startup if required",
			storeType:   kv.Env,
			required:    true,
			factoryErr:  errors.New("bad config content"),
			expectError: true,
		},
		{
			name:        "factory generation failure skipped if optional",
			storeType:   kv.Env,
			required:    false,
			factoryErr:  errors.New("bad config content"),
			expectError: false,
		},
		{
			name:        "network init phase failure blocks startup if required",
			storeType:   kv.Env,
			required:    true,
			initErr:     errors.New("vault unreachable"),
			expectError: true,
		},
		{
			name:        "network init phase failure skipped if not required",
			storeType:   kv.Env,
			required:    false,
			initErr:     errors.New("consul dead"),
			expectError: false,
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()

			// Adding factory for env provider
			err := reg.Add(kv.Env, func(cfg json.RawMessage) (kv.Provider, error) {
				if tc.factoryErr != nil {
					return nil, tc.factoryErr
				}

				return &mockProvider{
					initFunc: func(ctx context.Context) error {
						return tc.initErr
					},
				}, nil
			})
			require.NoError(t, err)

			config := &kv.Config{
				Stores: map[string]kv.StoreConfig{
					"target-store": {
						Type:     tc.storeType,
						Required: tc.required,
					},
				},
			}

			err = reg.InitStores(t.Context(), config)
			if tc.expectError {
				require.Error(t, err)
			}
		})
	}
}

func TestInitStores_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("returns nil early without initializing stores when config is empty", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("mock", newFactory(nil, nil))
		require.NoError(t, err)

		err = r.InitStores(t.Context(), nil)
		assert.NoError(t, err)

		err = r.InitStores(t.Context(), &kv.Config{})
		assert.NoError(t, err)

		assert.False(t, r.isInitialized.Load())
	})

	t.Run("returns error if no factory provided", func(t *testing.T) {
		r := NewRegistry()
		err := r.InitStores(t.Context(), &kv.Config{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "factories must be added before initialize stores")
		require.False(t, r.isInitialized.Load())
	})

	t.Run("should be called once unless Close() was called", func(t *testing.T) {
		reg := NewRegistry()

		err := reg.Add("mock", newFactory(nil, nil))
		require.NoError(t, err)

		config := &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"target-store": {
					Type:     "mock",
					Required: true,
				},
			},
		}

		err = reg.InitStores(t.Context(), config)
		require.NoError(t, err)

		err = reg.InitStores(t.Context(), config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "stores have been initialized")

		err = reg.Close(t.Context())
		require.NoError(t, err)

		err = reg.InitStores(t.Context(), config)
		require.NoError(t, err)
	})

	t.Run("should close temporaly added stores when required store failed", func(t *testing.T) {
		// We have to iterate over until valid store is initialized because
		// we have map non-deterministic iteration order.
		for {
			r := NewRegistry()

			var validInitialized bool
			var closeFuncCalled bool
			validFactory := newFactory(func(ctx context.Context) error {
				validInitialized = true
				return nil
			}, func(ctx context.Context) error {
				closeFuncCalled = true
				return nil
			})

			err := r.Add("valid", validFactory)
			require.NoError(t, err)

			invalidFactory := newFactory(func(ctx context.Context) error {
				return errors.New("init error")
			}, nil)
			err = r.Add("invalid", invalidFactory)
			require.NoError(t, err)

			err = r.InitStores(t.Context(), &kv.Config{
				Stores: map[string]kv.StoreConfig{
					"valid-1":   {Type: "valid", Required: true},
					"invalid-1": {Type: "invalid", Required: true},
				},
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to initialize store")

			if validInitialized {
				require.True(
					t,
					closeFuncCalled,
					"expected temporarily added store to be closed, but Close was not called",
				)
				require.False(t, r.isInitialized.Load())

				break
			}
		}
	})

	t.Run("should handle error returned by secret store wrapper", func(t *testing.T) {
		r := NewRegistry()

		err := r.Add("valid", newFactory(nil, nil))
		require.NoError(t, err)

		err = r.InitStores(t.Context(), &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"valid-1": {Type: "valid", Required: true},
			},
			Cache: kv.CacheConfig{
				Enabled: true,
				TTL:     "-10s",
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to wrap store")
		require.False(t, r.isInitialized.Load())
	})

	t.Run("should log warning when optional store failed one of the steps", func(t *testing.T) {
		l := &mockLogger{}
		r := NewRegistry(WithLogger(l))

		err := r.Add("valid", newFactory(nil, nil))
		require.NoError(t, err)

		err = r.InitStores(t.Context(), &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"valid-1": {Type: "valid", Required: false},
			},
			Cache: kv.CacheConfig{
				Enabled: true,
				TTL:     "-10s",
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, l.warnCalls)
	})

	t.Run("should skip secret store wrapping if provider is standalone", func(t *testing.T) {
		r := NewRegistry()

		err := r.Add("valid", func(_ json.RawMessage) (kv.Provider, error) {
			return &mockProvider{isStandalone: true}, nil
		})
		require.NoError(t, err)

		err = r.InitStores(t.Context(), &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"valid-1": {Type: "valid", Required: true},
			},
		})
		require.NoError(t, err)
		require.Len(t, r.stores, 1)
	})

	t.Run("should initialize stores concurrently", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := NewRegistry()

			err := r.Add("valid", newFactory(func(ctx context.Context) error {
				time.Sleep(10 * time.Second)
				return nil
			}, nil))
			require.NoError(t, err)

			start := time.Now()
			err = r.InitStores(t.Context(), &kv.Config{
				Stores: map[string]kv.StoreConfig{
					"valid-1": {Type: "valid", Required: true},
					"valid-2": {Type: "valid", Required: true},
					"valid-3": {Type: "valid", Required: true},
					"valid-4": {Type: "valid", Required: true},
					"valid-5": {Type: "valid", Required: true},
				},
			})
			require.NoError(t, err)
			synctest.Wait()

			elapsed := time.Since(start)

			require.Equal(t, 10*time.Second, elapsed)
			require.Len(t, r.stores, 5)
		})
	})
}

func TestClose(t *testing.T) {
	t.Parallel()

	t.Run("can be called multiple times without error", func(t *testing.T) {
		reg := NewRegistry()
		err := reg.Close(t.Context())
		require.NoError(t, err)
		err = reg.Close(t.Context())
		require.NoError(t, err)
		err = reg.Close(t.Context())
		require.NoError(t, err)
	})

	t.Run("aggregates multiple errors", func(t *testing.T) {
		reg := NewRegistry()

		err := reg.Add("mock", func(cfg json.RawMessage) (kv.Provider, error) {
			return &mockProvider{
				closeFunc: func(ctx context.Context) error {
					return errors.New("cleanup failed")
				},
			}, nil
		})
		require.NoError(t, err)

		config := &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"store-1": {Type: "mock", Required: true},
				"store-2": {Type: "mock", Required: true},
			},
		}

		err = reg.InitStores(t.Context(), config)
		require.NoError(t, err)

		closeErr := reg.Close(t.Context())
		require.Error(t, closeErr)

		if err, ok := closeErr.(interface{ Unwrap() []error }); ok {
			er := err.Unwrap()
			require.Len(t, er, 2)
		} else {
			t.Error("close error must be a result of errors.Join which implements Unwrap")
		}
	})
}

func TestRegistry_Concurrency(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()

	err := reg.Add("static-type", newFactory(nil, nil))
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Go(func() {
		err := reg.Add(kv.Env, newFactory(nil, nil))
		require.NoError(t, err)
	})

	wg.Go(func() {
		_, err := reg.GetStore("any-store")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	wg.Wait()
}

func TestConcurrentInitStoresAndCloseAreHandledCorrectly(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()

	inInit := make(chan struct{})
	closeDone := make(chan struct{})

	err := reg.Add("mock", newFactory(func(ctx context.Context) error {
		close(inInit)
		<-closeDone

		return nil
	}, nil))
	require.NoError(t, err)

	var wg sync.WaitGroup

	// While InitStores is running another goroutine calls the Close method
	wg.Go(func() {
		config := &kv.Config{
			Stores: map[string]kv.StoreConfig{
				"store-1": {Type: "mock", Required: true},
			},
		}

		err := reg.InitStores(t.Context(), config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "registry was closed during initialization")
	})

	wg.Go(func() {
		<-inInit

		err := reg.Close(t.Context())
		require.NoError(t, err)

		close(closeDone)
	})

	wg.Wait()
}

func TestInitStores_CacheCleanupSurvivesInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRegistry()
		t.Cleanup(func() {
			r.Close(t.Context())
		})

		p := &mockProvider{}
		err := r.Add("test", func(config json.RawMessage) (kv.Provider, error) {
			return p, nil

		})
		require.NoError(t, err)

		cfg := &kv.Config{
			Cache: kv.CacheConfig{
				Enabled: true,
				TTL:     "1s",
			},
			Stores: map[string]kv.StoreConfig{
				"test-store": {Type: "test", Required: true},
			},
		}

		err = r.InitStores(t.Context(), cfg)
		require.NoError(t, err)

		store, err := r.GetStore("test-store")
		require.NoError(t, err)

		// Populate cache
		val, err := store.Get(t.Context(), "key1")
		require.NoError(t, err)
		require.Equal(t, "value", val)
		require.Equal(t, int32(1), p.calls.Load())

		// Second call should hit cache (no provider call)
		_, err = store.Get(t.Context(), "key1")
		require.NoError(t, err)
		require.Equal(t, int32(1), p.calls.Load(), "should hit cache")

		time.Sleep(time.Second)
		synctest.Wait()

		_, err = store.Get(t.Context(), "key1")
		require.NoError(t, err)
		require.Equal(t, int32(2), p.calls.Load(), "cache should have cleaned up expired entry")
	})
}
