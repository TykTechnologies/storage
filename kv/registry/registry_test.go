package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
)

type mockProvider struct {
	initFunc  func(ctx context.Context) error
	closeFunc func(ctx context.Context) error
}

func (m *mockProvider) Get(ctx context.Context, key string) (string, error) {
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

var (
	_ kv.Provider    = (*mockProvider)(nil)
	_ kv.Initializer = (*mockProvider)(nil)
	_ kv.Closer      = (*mockProvider)(nil)
)

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

		var wg sync.WaitGroup

		for i := range 10 {
			wg.Go(func() {
				err := r.Add(fmt.Sprintf("env-%d", i), newFactory(nil, nil))
				require.NoError(t, err)
			})
		}

		wg.Wait()

		require.Len(t, r.factories, 10)
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
		storeType   string
		required    bool
		factoryErr  error
		initErr     error
		expectError bool
	}

	table := []testCase{
		{
			name:        "required store initializes perfectly",
			storeType:   "mock",
			required:    true,
			expectError: false,
		},
		{
			name:        "unregistered provider type fails if required",
			storeType:   "unknown",
			required:    true,
			expectError: true,
		},
		{
			name:        "unregistered provider type skipped if optional",
			storeType:   "unknown",
			required:    false,
			expectError: false,
		},
		{
			name:        "factory generation failure blocks startup if required",
			storeType:   "mock",
			required:    true,
			factoryErr:  errors.New("bad config content"),
			expectError: true,
		},
		{
			name:        "factory generation failure skipped if optional",
			storeType:   "mock",
			required:    false,
			factoryErr:  errors.New("bad config content"),
			expectError: false,
		},
		{
			name:        "network init phase failure blocks startup if required",
			storeType:   "mock",
			required:    true,
			initErr:     errors.New("vault unreachable"),
			expectError: true,
		},
		{
			name:        "network init phase failure skipped if not required",
			storeType:   "mock",
			required:    false,
			initErr:     errors.New("consul dead"),
			expectError: false,
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()

			err := reg.Add("mock", func(cfg json.RawMessage) (kv.Provider, error) {
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
						Config:   json.RawMessage(`{"timeout": "2s"}`),
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
					Config:   json.RawMessage(`{"timeout": "2s"}`),
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
			done := make(chan struct{})
			validFactory := newFactory(func(ctx context.Context) error {
				validInitialized = true
				return nil
			}, func(ctx context.Context) error {
				close(done)
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
				select {
				case <-done:
				default:
					t.Fatal("expected temporarily added store to be closed, but Close was not called")
				}

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
}

func TestRegistry_Close_ErrorAggregation(t *testing.T) {
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

	err = reg.InitStores(context.Background(), config)
	require.NoError(t, err)

	closeErr := reg.Close(t.Context())
	require.Error(t, closeErr)

	if err, ok := closeErr.(interface{ Unwrap() []error }); ok {
		er := err.Unwrap()
		require.Len(t, er, 2)
	} else {
		t.Error("close error must be a result of errors.Join which implements Unwrap")
	}
}

func TestRegistry_Concurrency(t *testing.T) {
	reg := NewRegistry()

	err := reg.Add("static-type", newFactory(nil, nil))
	require.NoError(t, err)

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Go(func() {
			err := reg.Add(fmt.Sprintf("type-%d", i), newFactory(nil, nil))
			require.NoError(t, err)
		})

		wg.Go(func() {
			_, err := reg.GetStore("any-store")
			require.ErrorIs(t, err, kv.ErrStoreNotFound)
		})
	}

	wg.Wait()
}
