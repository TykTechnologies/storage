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

func newFactory(initFunc, closeFunc func(ctx context.Context) error, err error) kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		return &mockProvider{
			initFunc:  initFunc,
			closeFunc: closeFunc,
		}, err
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

func TestAddFactory(t *testing.T) {
	t.Parallel()

	t.Run("successful registration", func(t *testing.T) {
		r := NewRegistry()

		var wg sync.WaitGroup

		for i := range 10 {
			wg.Go(func() {
				err := r.Add(fmt.Sprintf("env-%d", i), newFactory(nil, nil, nil))
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
		err := r.Add("env", newFactory(nil, nil, nil))
		require.NoError(t, err)

		err = r.Add("env", newFactory(nil, nil, nil))
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

	t.Run("returns error if no factory provided", func(t *testing.T) {
		r := NewRegistry()
		err := r.InitStores(t.Context(), &kv.Config{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "factories must be added before initialize stores")
	})

	t.Run("should be called once unless Close() was called", func(t *testing.T) {
		reg := NewRegistry()

		err := reg.Add("mock", func(cfg json.RawMessage) (kv.Provider, error) {
			return &mockProvider{}, nil
		})
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

	err := reg.Add("static-type", newFactory(nil, nil, nil))
	require.NoError(t, err)

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Go(func() {
			err := reg.Add(fmt.Sprintf("type-%d", i), newFactory(nil, nil, nil))
			require.NoError(t, err)
		})

		wg.Go(func() {
			_, err := reg.GetStore("any-store")
			require.ErrorIs(t, err, kv.ErrStoreNotFound)
		})
	}

	wg.Wait()
}
