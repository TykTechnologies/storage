package registry

import (
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()
	require.NotNil(t, registry)
	require.NotNil(t, registry.stores)
	require.NotNil(t, registry.factories)
}

// Test: Adding factory is working with concurrent request. Returns error if provider type is not valid.
// Test: InitStores are calling provider factory with passing config.
// Wrap specific providers with secret store and call Init methods if its present.
// Returns error if

func TestAddFactory(t *testing.T) {
	t.Run("returns an error if provider type is empty", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be empty")

		require.Len(t, r.factories, 0)
	})

	t.Run("returns an error if factory is already registered for the type", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("env", newFactory())
		require.NoError(t, err)

		err = r.Add("env", newFactory())
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already provided")

		require.Len(t, r.factories, 1)
	})

	t.Run("returns an error if factory is nil", func(t *testing.T) {
		r := NewRegistry()
		err := r.Add("env", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "factory cannot be nil")
		require.Len(t, r.factories, 0)
	})

	t.Run("stores factories correctly in concurrent env", func(t *testing.T) {})
}

// InitStores should probably be called only once.
func TestInitStores(t *testing.T) {
	t.Run("returns an error if called with empty config map", func(t *testing.T) {})

	t.Run("returns an error if called more than once", func(t *testing.T) {})

	t.Run("returns an error if Init() func failed and 'required: true' is set", func(t *testing.T) {})
}

func newFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		return nil, nil
	}
}
