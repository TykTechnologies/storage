package resolve_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProvider struct {
	value   string
	err     error
	lastKey string
}

func (m *mockProvider) Get(_ context.Context, key string) (string, error) {
	m.lastKey = key
	return m.value, m.err
}

type mockStoreGetter struct {
	stores map[string]kv.Provider
}

func (m *mockStoreGetter) GetStore(name string) (kv.Provider, error) {
	p, ok := m.stores[name]
	if !ok {
		return nil, kv.NewStoreNotFoundError(name)
	}

	return p, nil
}

func newGetter(stores map[string]kv.Provider) kv.StoreGetter {
	return &mockStoreGetter{stores: stores}
}

func TestResolve(t *testing.T) {
	t.Parallel()

	const jsonCreds = `{"username":"admin","password":"hunter2"}`

	tests := []struct {
		name    string
		stores  map[string]kv.Provider
		input   string
		want    string
		wantErr error
	}{
		// plain strings
		{
			name:  "plain string passes through unchanged",
			input: "just-a-plain-string",
			want:  "just-a-plain-string",
		},
		{
			name:  "empty string passes through unchanged",
			input: "",
			want:  "",
		},
		// kv:// whole-value
		{
			name:   "whole-value replacement",
			stores: map[string]kv.Provider{"vault": &mockProvider{value: "s3cr3t"}},
			input:  "kv://vault/db/password",
			want:   "s3cr3t",
		},
		{
			name:   "whole-value with fragment",
			stores: map[string]kv.Provider{"vault": &mockProvider{value: jsonCreds}},
			input:  "kv://vault/db/creds#password",
			want:   "hunter2",
		},
		// $kv{} inline
		{
			name:   "single inline token",
			stores: map[string]kv.Provider{"env": &mockProvider{value: "myhost.com"}},
			input:  "https://$kv{env:API_HOST}/v1",
			want:   "https://myhost.com/v1",
		},
		{
			name:   "multiple inline tokens",
			stores: map[string]kv.Provider{"env": &mockProvider{value: "resolved"}},
			input:  "$kv{env:A}/$kv{env:B}/$kv{env:C}",
			want:   "resolved/resolved/resolved",
		},
		{
			name:   "inline token with fragment",
			stores: map[string]kv.Provider{"vault": &mockProvider{value: `{"host":"db.internal"}`}},
			input:  "postgres://$kv{vault:db/creds#host}:5432/mydb",
			want:   "postgres://db.internal:5432/mydb",
		},
		// JSON Pointer traversal
		{
			name:   "nested object field",
			stores: map[string]kv.Provider{"vault": &mockProvider{value: `{"db":{"host":"localhost","port":5432}}`}},
			input:  "kv://vault/secret#/db/host",
			want:   "localhost",
		},
		{
			name:   "array index",
			stores: map[string]kv.Provider{"consul": &mockProvider{value: `{"hosts":["primary","replica"]}`}},
			input:  "kv://consul/service/db#/hosts/1",
			want:   "replica",
		},
		{
			name:   "RFC 6901 escaped segments",
			stores: map[string]kv.Provider{"env": &mockProvider{value: `{"a/b":{"c~d":"value"}}`}},
			input:  "kv://env/KEY#/a~1b/c~0d",
			want:   "value",
		},
		// no recursive resolution
		{
			name:   "resolved value containing kv syntax is not re-resolved",
			stores: map[string]kv.Provider{"env": &mockProvider{value: "kv://vault/another-secret"}},
			input:  "kv://env/SOME_KEY",
			want:   "kv://vault/another-secret",
		},
		// errors
		{
			name:    "store not found",
			input:   "kv://nonexistent/key",
			wantErr: kv.ErrStoreNotFound,
		},
		{
			name:    "invalid JSON when fragment requested",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: "not-json"}},
			input:   "kv://vault/secret#field",
			wantErr: resolve.ErrInvalidJSON,
		},
		{
			name:    "field not found in JSON payload",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: `{"password":"secret"}`}},
			input:   "kv://vault/secret#username",
			wantErr: resolve.ErrFieldNotFound,
		},
		{
			name:    "inline token store not found",
			input:   "prefix-$kv{ghost:key}-suffix",
			wantErr: kv.ErrStoreNotFound,
		},
		{
			name:    "malformed kv:// missing slash",
			input:   "kv://nopath",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed $kv{} missing colon",
			input:   "$kv{nocolon}",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed kv:// empty path",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: "x"}},
			input:   "kv://vault/",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed kv:// empty store name",
			input:   "kv:///path",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed kv:// empty path with fragment",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: "x"}},
			input:   "kv://vault/#field",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed $kv{} empty path",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: "x"}},
			input:   "$kv{vault:}",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed $kv{} empty store name",
			input:   "$kv{:path}",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed kv:// empty store name and path",
			input:   "kv:///",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed $kv{} empty store name and path",
			input:   "$kv{:}",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed $kv{} empty path with fragment",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: "x"}},
			input:   "$kv{vault:#field}",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:   "whole-value takes precedence over inline token in path",
			stores: map[string]kv.Provider{"vault": &mockProvider{value: "resolved"}},
			input:  "kv://vault/$kv{env:SUFFIX}",
			want:   "resolved",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := resolve.NewResolver(newGetter(tc.stores))
			got, err := r.Resolve(t.Context(), tc.input)

			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolve_PathRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantKey string
	}{
		{
			name:    "kv:// single-segment path",
			input:   "kv://vault/secret",
			wantKey: "secret",
		},
		{
			name:    "kv:// multi-segment path",
			input:   "kv://vault/db/creds",
			wantKey: "db/creds",
		},
		{
			name:    "kv:// path with fragment stripped before Get",
			input:   "kv://vault/db/creds#password",
			wantKey: "db/creds",
		},
		{
			name:    "inline $kv{} path",
			input:   "$kv{vault:db/creds}",
			wantKey: "db/creds",
		},
		{
			name:    "inline $kv{} path with fragment stripped before Get",
			input:   "$kv{vault:db/creds#password}",
			wantKey: "db/creds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider := &mockProvider{value: `{"password":"secret"}`}
			getter := &mockStoreGetter{stores: map[string]kv.Provider{"vault": provider}}
			r := resolve.NewResolver(getter)

			_, err := r.Resolve(t.Context(), tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantKey, provider.lastKey)
		})
	}
}

func TestResolve_MultipleFailedTokens_AllReported(t *testing.T) {
	t.Parallel()

	r := resolve.NewResolver(newGetter(nil))

	_, err := r.Resolve(t.Context(), "$kv{ghost1:a}/$kv{ghost2:b}")

	require.Error(t, err)
	assert.ErrorIs(t, err, kv.ErrStoreNotFound)
	assert.Contains(t, err.Error(), "ghost1")
	assert.Contains(t, err.Error(), "ghost2")
}

func TestResolve_MixedFailedAndResolvableTokens_ErrorWins(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "ok"},
	})
	r := resolve.NewResolver(getter)

	got, err := r.Resolve(t.Context(), "$kv{env:GOOD}/$kv{ghost:bad}")

	assert.ErrorIs(t, err, kv.ErrStoreNotFound)
	assert.Empty(t, got)
}

func TestResolve_KeyNotFound(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{err: &kv.KeyNotFoundError{StoreName: "env", KeyPath: "MISSING"}},
	})
	r := resolve.NewResolver(getter)

	_, err := r.Resolve(t.Context(), "kv://env/MISSING")

	var target *kv.KeyNotFoundError
	assert.ErrorAs(t, err, &target)
}

func TestResolve_WholeValue_FragmentNormalization(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: `{"password":"secret"}`},
	})
	r := resolve.NewResolver(getter)

	withoutSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#password")
	assert.NoError(t, err)

	withSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#/password")
	assert.NoError(t, err)

	assert.Equal(t, withoutSlash, withSlash)
	assert.Equal(t, "secret", withSlash)
}

func TestResolve_Inline_FragmentNormalization(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: `{"password":"secret"}`},
	})
	r := resolve.NewResolver(getter)

	withoutSlash, err := r.Resolve(t.Context(), "prefix-$kv{env:MY_KEY#password}-suffix")
	assert.NoError(t, err)

	withSlash, err := r.Resolve(t.Context(), "prefix-$kv{env:MY_KEY#/password}-suffix")
	assert.NoError(t, err)

	assert.Equal(t, withoutSlash, withSlash)
	assert.Equal(t, "prefix-secret-suffix", withSlash)
}

func TestResolve_JSONPointer_ObjectLeaf_ReserializedAsJSON(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"db":{"host":"localhost","port":5432}}`},
	})
	r := resolve.NewResolver(getter)

	got, err := r.Resolve(t.Context(), "kv://vault/secret#/db")
	assert.NoError(t, err)

	var reserialized map[string]any
	assert.NoError(t, json.Unmarshal([]byte(got), &reserialized))
	assert.Equal(t, "localhost", reserialized["host"])
	assert.Equal(t, float64(5432), reserialized["port"])
}

func TestResolveAll_FlatDocument(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "resolved-value"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"host":"kv://env/HOST","port":"5432"}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "resolved-value", result["host"])
	assert.Equal(t, "5432", result["port"])
}

func TestResolveAll_MixedSyntax(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{value: "resolved"}
	getter := newGetter(map[string]kv.Provider{"env": provider})
	r := resolve.NewResolver(getter)

	input := []byte(`{
        "host": "kv://env/HOST",
        "dsn":  "postgres://$kv{env:USER}@localhost/db"
    }`)

	got, err := r.ResolveAll(t.Context(), input)
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "resolved", result["host"])
	assert.Equal(t, "postgres://resolved@localhost/db", result["dsn"])
}

func TestResolveAll_NestedObjectAndArray(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "deep-value"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"outer":{"inner":"kv://env/KEY","list":["kv://env/A","plain"]}}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	type nested struct {
		Outer struct {
			Inner string   `json:"inner"`
			List  []string `json:"list"`
		} `json:"outer"`
	}
	var result nested
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "deep-value", result.Outer.Inner)
	assert.Equal(t, "deep-value", result.Outer.List[0])
	assert.Equal(t, "plain", result.Outer.List[1])
}

func TestResolveAll_NonStringValuesPreserved(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "ok"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"flag":true,"count":42,"nothing":null,"name":"kv://env/NAME"}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, true, result["flag"])
	assert.Equal(t, float64(42), result["count"])
	assert.Nil(t, result["nothing"])
	assert.Equal(t, "ok", result["name"])
}

func TestResolveAll_NoKVReferences_ReturnedUnchanged(t *testing.T) {
	t.Parallel()

	r := resolve.NewResolver(newGetter(nil))

	input := []byte(`{"host":"localhost","port":5432}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "localhost", result["host"])
	assert.Equal(t, float64(5432), result["port"])
}

func TestResolveAll_EmptyDocument(t *testing.T) {
	t.Parallel()

	r := resolve.NewResolver(newGetter(nil))

	got, err := r.ResolveAll(t.Context(), []byte(`{}`))
	assert.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Empty(t, result)
}

func TestResolveAll_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stores  map[string]kv.Provider
		input   []byte
		wantErr error
	}{
		{
			name:    "unresolvable kv reference returns store error",
			input:   []byte(`{"a":"kv://missing/key","b":"plain"}`),
			wantErr: kv.ErrStoreNotFound,
		},
		{
			name:    "invalid JSON input returns error",
			input:   []byte(`not json containing kv://store/key`),
			wantErr: resolve.ErrInvalidJSON,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := resolve.NewResolver(newGetter(tc.stores))
			_, err := r.ResolveAll(t.Context(), tc.input)

			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
		})
	}
}

func TestResolveAll_LargeIntegersPreserved(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "resolved"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"id":9007199254740993,"nested":{"ts":1749600000000000001},"host":"kv://env/HOST"}`)

	got, err := r.ResolveAll(t.Context(), input)
	require.NoError(t, err)

	assert.Contains(t, string(got), "9007199254740993")
	assert.Contains(t, string(got), "1749600000000000001")
	assert.Contains(t, string(got), `"host":"resolved"`)
}

func TestResolveAll_FloatsPreserved(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "resolved"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"ratio":0.25,"host":"kv://env/HOST"}`)

	got, err := r.ResolveAll(t.Context(), input)
	require.NoError(t, err)

	assert.Contains(t, string(got), "0.25")
}

func TestResolveAll_InvalidJSONWithoutRefs_ReturnsError(t *testing.T) {
	t.Parallel()

	r := resolve.NewResolver(newGetter(nil))

	_, err := r.ResolveAll(t.Context(), []byte(`{definitely not json`))
	assert.ErrorIs(t, err, resolve.ErrInvalidJSON)
}

func TestResolveAll_ValidJSONWithoutRefs_ByteIdentical(t *testing.T) {
	t.Parallel()

	r := resolve.NewResolver(newGetter(nil))

	input := []byte("{\n  \"z\": 1,\n  \"a\": \"plain\"\n}")

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, input, got)
}

func TestExtractJSONPointer_LargeIntegerLeaf(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"big_id":9007199254740993}`},
	})
	r := resolve.NewResolver(getter)

	got, err := r.Resolve(t.Context(), "kv://vault/secret#big_id")
	require.NoError(t, err)
	assert.Equal(t, "9007199254740993", got)
}

func TestResolveAll_NoHTMLEscaping(t *testing.T) {
	t.Parallel()

	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "https://upstream.internal/?a=1&b=<2>"},
	})
	r := resolve.NewResolver(getter)

	input := []byte(`{"url":"https://example.com/?x=1&y=2","target":"kv://env/UPSTREAM"}`)

	got, err := r.ResolveAll(t.Context(), input)
	require.NoError(t, err)

	assert.Contains(t, string(got), "https://example.com/?x=1&y=2")
	assert.Contains(t, string(got), "https://upstream.internal/?a=1&b=<2>")
	assert.NotContains(t, string(got), `\u0026`)
	assert.NotContains(t, string(got), `\u003c`)
}

// --------------------------------------------------------------------------
// Benchmarks
// --------------------------------------------------------------------------

func BenchmarkResolve_ThreeInlineTokensWithFragment(b *testing.B) {
	payload := `{"host":"db.internal","port":"5432"}`
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: payload},
		"env":   &mockProvider{value: "simple-value"},
	})
	r := resolve.NewResolver(getter)
	input := "postgres://$kv{vault:db/creds#host}:$kv{vault:db/creds#port}/$kv{env:DB_NAME}"
	ctx := context.Background()

	b.ResetTimer()

	for b.Loop() {
		_, err := r.Resolve(ctx, input)
		if err != nil {
			b.Fatal(err)
		}
	}
}
