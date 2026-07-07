package inline_test

import (
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/inline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newProvider builds the provider through its factory with the given config,
// exactly as the registry would.
func newProvider(t *testing.T, cfg inline.Config) kv.Provider {
	t.Helper()

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	p, err := inline.NewFactory()(raw)
	require.NoError(t, err)
	require.NotNil(t, p)

	return p
}

func TestNewFactory(t *testing.T) {
	t.Parallel()

	t.Run("valid config builds a provider", func(t *testing.T) {
		t.Parallel()

		p, err := inline.NewFactory()(json.RawMessage(`{"data":{"token":"hvs.xxx"}}`))
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("empty or absent config builds a provider with an empty map", func(t *testing.T) {
		t.Parallel()

		for _, cfg := range []json.RawMessage{nil, {}, json.RawMessage(`{}`)} {
			p, err := inline.NewFactory()(cfg)
			require.NoError(t, err, "config %q", string(cfg))
			require.NotNil(t, p)
		}
	})

	t.Run("invalid JSON config errors", func(t *testing.T) {
		t.Parallel()

		_, err := inline.NewFactory()(json.RawMessage(`{not json`))
		require.Error(t, err)
	})
}

func TestProviderIsStandalone(t *testing.T) {
	t.Parallel()

	p := newProvider(t, inline.Config{})

	standalone, ok := kv.AsStandalone(p)
	require.True(t, ok, "inline provider must implement Standalone")
	require.True(t, standalone.IsStandalone(),
		"inline data is in-memory and literal — no cache wrapper")
}

func TestProviderGet(t *testing.T) {
	t.Parallel()

	t.Run("returns the stored value for a present key", func(t *testing.T) {
		t.Parallel()

		got, err := newProvider(t, inline.Config{Data: map[string]string{"token": "hvs.xxx"}}).
			Get(t.Context(), "token")
		require.NoError(t, err)
		assert.Equal(t, "hvs.xxx", got)
	})

	t.Run("a key present with an empty value returns empty string and no error", func(t *testing.T) {
		t.Parallel()

		got, err := newProvider(t, inline.Config{Data: map[string]string{"blank": ""}}).
			Get(t.Context(), "blank")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("a missing key returns kv.KeyNotFoundError", func(t *testing.T) {
		t.Parallel()

		_, err := newProvider(t, inline.Config{Data: map[string]string{"present": "v"}}).
			Get(t.Context(), "absent")

		var notFound *kv.KeyNotFoundError
		require.ErrorAs(t, err, &notFound)
		assert.Equal(t, "absent", notFound.KeyPath)
	})

	t.Run("keys match exactly: no case folding", func(t *testing.T) {
		t.Parallel()

		p := newProvider(t, inline.Config{Data: map[string]string{"Foo": "bar"}})

		got, err := p.Get(t.Context(), "Foo")
		require.NoError(t, err)
		assert.Equal(t, "bar", got)

		_, err = p.Get(t.Context(), "foo")
		var notFound *kv.KeyNotFoundError
		require.ErrorAs(t, err, &notFound, "lookup is case-sensitive, no transformation")
	})

	t.Run("values are returned verbatim: a kv:// string is not resolved", func(t *testing.T) {
		t.Parallel()

		got, err := newProvider(t, inline.Config{Data: map[string]string{"ref": "kv://vault/secret#field"}}).
			Get(t.Context(), "ref")
		require.NoError(t, err)
		assert.Equal(t, "kv://vault/secret#field", got)
	})

	t.Run("nil/empty data map yields not-found for any key without panicking", func(t *testing.T) {
		t.Parallel()

		for _, cfg := range []inline.Config{{}, {Data: map[string]string{}}} {
			_, err := newProvider(t, cfg).Get(t.Context(), "anything")
			var notFound *kv.KeyNotFoundError
			require.ErrorAs(t, err, &notFound)
		}
	})

	t.Run("empty key errors", func(t *testing.T) {
		t.Parallel()

		_, err := newProvider(t, inline.Config{Data: map[string]string{"present": "v"}}).
			Get(t.Context(), "")
		require.ErrorIs(t, err, inline.ErrEmptyKey)
	})
}
