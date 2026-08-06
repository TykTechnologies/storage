// This file is a facade-level smoke test:
// it proves the public API delegates to the engine and
// that the re-exported error sentinels keep their identity, so
// errors.Is works for external callers.
package resolver_test

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/require"
)

type stubProvider struct {
	value string
	err   error
}

func (s stubProvider) Get(_ context.Context, _ string) (string, error) {
	return s.value, s.err
}

type stubStores map[string]kv.Provider

func (s stubStores) GetStore(name string) (kv.Provider, error) {
	p, ok := s[name]
	if !ok {
		return nil, kv.NewStoreNotFoundError(name)
	}

	return p, nil
}

func TestNewResolverDelegatesToEngine(t *testing.T) {
	t.Parallel()

	res := resolver.NewResolver(stubStores{
		"vault": stubProvider{value: `{"password":"hunter2"}`},
		"env":   stubProvider{value: "myhost.com"},
	})

	t.Run("whole-value reference", func(t *testing.T) {
		t.Parallel()

		got, err := res.Resolve(t.Context(), "kv://env/API_HOST")
		require.NoError(t, err)
		require.Equal(t, "myhost.com", got)
	})

	t.Run("whole-value reference with fragment extraction", func(t *testing.T) {
		t.Parallel()

		got, err := res.Resolve(t.Context(), "kv://vault/db/creds#password")
		require.NoError(t, err)
		require.Equal(t, "hunter2", got)
	})

	t.Run("inline token inside a larger string", func(t *testing.T) {
		t.Parallel()

		got, err := res.Resolve(t.Context(), "https://$kv{env:API_HOST}/v1")
		require.NoError(t, err)
		require.Equal(t, "https://myhost.com/v1", got)
	})

	t.Run("ResolveAll resolves string values across a document", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{"url":"https://$kv{env:API_HOST}/v1","secret":"kv://vault/db/creds#password","port":8080}`)

		got, err := res.ResolveAll(t.Context(), doc)
		require.NoError(t, err)
		require.JSONEq(t, `{"url":"https://myhost.com/v1","secret":"hunter2","port":8080}`, string(got))
	})

	t.Run("ResolveAll returns document without KV syntax unchanged", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{"plain": "value", "n": 1}`)

		got, err := res.ResolveAll(t.Context(), doc)
		require.NoError(t, err)
		require.Equal(t, doc, got)
	})
}

func TestPublicErrorSentinelsSurviveTheFacade(t *testing.T) {
	t.Parallel()

	res := resolver.NewResolver(stubStores{
		"vault": stubProvider{value: `{"user":"admin"}`},
	})

	t.Run("malformed reference matches resolver.ErrMalformedReference", func(t *testing.T) {
		t.Parallel()

		_, err := res.Resolve(t.Context(), "kv://no-path-separator")
		require.ErrorIs(t, err, resolver.ErrMalformedReference)
	})

	t.Run("missing JSON field matches resolver.ErrFieldNotFound", func(t *testing.T) {
		t.Parallel()

		_, err := res.Resolve(t.Context(), "kv://vault/creds#missing_field")
		require.ErrorIs(t, err, resolver.ErrFieldNotFound)
	})

	t.Run("invalid document matches resolver.ErrInvalidJSON", func(t *testing.T) {
		t.Parallel()

		_, err := res.ResolveAll(t.Context(), []byte(`{not json`))
		require.ErrorIs(t, err, resolver.ErrInvalidJSON)
	})

	t.Run("unknown store matches kv.ErrStoreNotFound", func(t *testing.T) {
		t.Parallel()

		_, err := res.Resolve(t.Context(), "kv://absent/some/key")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})
}

func TestProviderErrorsPropagateThroughFacade(t *testing.T) {
	t.Parallel()

	keyErr := &kv.KeyNotFoundError{StoreName: "vault", KeyPath: "db/creds"}
	res := resolver.NewResolver(stubStores{
		"vault": stubProvider{err: keyErr},
	})

	_, err := res.Resolve(t.Context(), "kv://vault/db/creds")

	var got *kv.KeyNotFoundError
	require.ErrorAs(t, err, &got)
	require.Equal(t, "vault", got.StoreName)
}

func TestParseReference(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantErr bool
		want    resolver.Reference
	}{
		// --- valid kv:// references (ok=true, err=nil) ---
		{
			name:   "store and multi-segment path, no fragment",
			input:  "kv://vault/secret/tyk-apis",
			wantOK: true,
			want:   resolver.Reference{Store: "vault", Path: "secret/tyk-apis"},
		},
		{
			name:   "store, path and fragment",
			input:  "kv://vault/secret/tyk-apis#api_key",
			wantOK: true,
			want:   resolver.Reference{Store: "vault", Path: "secret/tyk-apis", Field: "api_key"},
		},
		{
			name:   "single-segment path",
			input:  "kv://env/MY_VAR",
			wantOK: true,
			want:   resolver.Reference{Store: "env", Path: "MY_VAR"},
		},
		{
			name:   "consul-style multi-segment path, no fragment",
			input:  "kv://consul/tyk-apis/edge/key",
			wantOK: true,
			want:   resolver.Reference{Store: "consul", Path: "tyk-apis/edge/key"},
		},
		{
			name:   "fragment carrying a JSON pointer is preserved verbatim",
			input:  "kv://vault/db/creds#data/password",
			wantOK: true,
			want:   resolver.Reference{Store: "vault", Path: "db/creds", Field: "data/password"},
		},
		{
			name:   "only the first hash splits path from fragment",
			input:  "kv://s/p#a#b",
			wantOK: true,
			want:   resolver.Reference{Store: "s", Path: "p", Field: "a#b"},
		},
		{
			name:   "trailing hash yields an empty fragment (valid, no field)",
			input:  "kv://vault/secret/app#",
			wantOK: true,
			want:   resolver.Reference{Store: "vault", Path: "secret/app"},
		},

		// --- not a kv:// whole-value reference (ok=false, err=nil) ---
		{
			name:  "legacy vault scheme is not a kv:// reference",
			input: "vault://secret/tyk-apis.api_key",
		},
		{
			name:  "legacy consul scheme is not a kv:// reference",
			input: "consul://tyk-apis/edge_key",
		},
		{
			name:  "plain literal is not a reference",
			input: "just-a-literal-value",
		},
		{
			name:  "inline token is not a whole-value reference",
			input: "$kv{vault:secret/tyk-apis#api_key}",
		},
		{
			name:  "empty string is not a reference",
			input: "",
		},

		// --- malformed kv:// references (ok=true, err wraps ErrMalformedReference) ---
		{
			name:    "missing path separator",
			input:   "kv://vaultonly",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "prefix only",
			input:   "kv://",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "empty store name",
			input:   "kv:///secret/x",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "empty path",
			input:   "kv://vault/",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "empty store and path",
			input:   "kv:///",
			wantOK:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := resolver.ParseReference(tt.input)

			require.Equal(t, tt.wantOK, ok)

			if tt.wantErr {
				require.ErrorIs(t, err, resolver.ErrMalformedReference,
					"a malformed kv:// reference must wrap ErrMalformedReference")

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
