package env_test

import (
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newProvider builds the provider through its factory with the given config,
// exactly as the registry would.
func newProvider(t *testing.T, cfg env.Config) kv.Provider {
	t.Helper()

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	p, err := env.NewFactory()(raw)
	require.NoError(t, err)
	require.NotNil(t, p)

	return p
}

func TestNewFactory(t *testing.T) {
	t.Parallel()

	t.Run("valid config builds a provider", func(t *testing.T) {
		t.Parallel()

		p, err := env.NewFactory()(json.RawMessage(`{"prefix":"TYK_SECRET_","uppercase":true}`))
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("empty or absent config builds a provider (env refs rejected until prefix is set)", func(t *testing.T) {
		t.Parallel()

		for _, cfg := range []json.RawMessage{nil, {}, json.RawMessage(`{}`)} {
			p, err := env.NewFactory()(cfg)
			require.NoError(t, err, "config %q", string(cfg))
			require.NotNil(t, p)
		}
	})

	t.Run("invalid JSON config errors", func(t *testing.T) {
		t.Parallel()

		_, err := env.NewFactory()(json.RawMessage(`{not json`))
		require.Error(t, err)
	})
}

func TestProviderIsStandaloner(t *testing.T) {
	t.Parallel()

	p := newProvider(t, env.Config{})

	standalone, ok := kv.AsStandaloner(p)
	require.True(t, ok, "env provider must implement Standalone")
	require.True(t, standalone.IsStandalone(),
		"env reads are in-process and cheap — no cache wrapper")
}

func TestProviderGet(t *testing.T) {
	t.Run("legacy compat: prefix + uppercase reads TYK_SECRET_<UPPER>", func(t *testing.T) {
		t.Setenv("TYK_SECRET_MY_KEY", "value")

		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "my_key")
		require.NoError(t, err)
		assert.Equal(t, "value", got)
	})

	t.Run("unset variable returns empty string and no error", func(t *testing.T) {
		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "definitely_not_set_var")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("missing variable never returns kv.KeyNotFoundError", func(t *testing.T) {
		_, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "definitely_not_set_var")
		require.NoError(t, err)

		var notFound *kv.KeyNotFoundError
		require.NotErrorAs(t, err, &notFound, "env must never report not-found")
	})

	t.Run("variable set to empty string returns empty string and no error", func(t *testing.T) {
		t.Setenv("TYK_SECRET_EMPTY_KEY", "")

		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "empty_key")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("uppercase false uses the key as-is (no case folding)", func(t *testing.T) {
		t.Setenv("TYK_SECRET_FOO", "bar")

		p := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: false})

		got, err := p.Get(t.Context(), "FOO")
		require.NoError(t, err)
		assert.Equal(t, "bar", got)

		got, err = p.Get(t.Context(), "foo")
		require.NoError(t, err)
		assert.Equal(t, "", got, "no case folding: lowercase key must not match TYK_SECRET_FOO")
	})

	t.Run("empty prefix is rejected by default: every key errors with ErrPrefixRequired", func(t *testing.T) {
		t.Setenv("BARE_KEY", "bare-value")

		// AllowNoPrefix defaults to false — the fail-closed guard.
		p := newProvider(t, env.Config{Prefix: "", Uppercase: true})

		for _, key := range []string{"bare_key", "BARE_KEY", ""} {
			_, err := p.Get(t.Context(), key)
			require.ErrorIs(t, err, env.ErrPrefixRequired, "key %q", key)
		}
	})

	t.Run("allow_no_prefix reads raw process env when the prefix is empty", func(t *testing.T) {
		t.Setenv("BARE_KEY", "bare-value")

		got, err := newProvider(t, env.Config{Prefix: "", Uppercase: true, AllowNoPrefix: true}).
			Get(t.Context(), "bare_key")
		require.NoError(t, err)
		assert.Equal(t, "bare-value", got)
	})

	t.Run("allow_no_prefix with uppercase false uses the key verbatim", func(t *testing.T) {
		t.Setenv("raw_lower", "x")

		got, err := newProvider(t, env.Config{AllowNoPrefix: true}).
			Get(t.Context(), "raw_lower")
		require.NoError(t, err)
		assert.Equal(t, "x", got)
	})

	t.Run("allow_no_prefix still returns empty string and no error for a missing var", func(t *testing.T) {
		got, err := newProvider(t, env.Config{AllowNoPrefix: true, Uppercase: true}).
			Get(t.Context(), "definitely_not_set_var")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("allow_no_prefix is inert when a prefix is set: the prefix still confines", func(t *testing.T) {
		t.Setenv("TYK_SECRET_K", "confined")
		t.Setenv("K", "bare")

		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true, AllowNoPrefix: true}).
			Get(t.Context(), "k")
		require.NoError(t, err)
		assert.Equal(t, "confined", got,
			"AllowNoPrefix must not disable a configured prefix")
	})

	t.Run("allow_no_prefix json tag is wired to the raw config", func(t *testing.T) {
		t.Setenv("RAW_TAG_KEY", "ok")

		p, err := env.NewFactory()(json.RawMessage(`{"allow_no_prefix":true,"uppercase":true}`))
		require.NoError(t, err)

		got, err := p.Get(t.Context(), "raw_tag_key")
		require.NoError(t, err)
		assert.Equal(t, "ok", got)
	})

	t.Run("prefix is literal: uppercase never applies to the prefix", func(t *testing.T) {
		t.Setenv("tyk_K", "lit")

		got, err := newProvider(t, env.Config{Prefix: "tyk_", Uppercase: true}).
			Get(t.Context(), "k")
		require.NoError(t, err)
		assert.Equal(t, "lit", got)
	})

	t.Run("value is returned byte-exact (no trailing-newline trim)", func(t *testing.T) {
		t.Setenv("TYK_SECRET_RAW", "line\n")

		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "raw")
		require.NoError(t, err)
		assert.Equal(t, "line\n", got)
	})

	t.Run("empty key with a prefix set returns empty string and no error (no key guard)", func(t *testing.T) {
		got, err := newProvider(t, env.Config{Prefix: "TYK_SECRET_", Uppercase: true}).
			Get(t.Context(), "")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})
}
