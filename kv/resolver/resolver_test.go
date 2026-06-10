package resolver_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProvider struct {
	value string
	err   error
}

func (m *mockProvider) Get(_ context.Context, _ string) (string, error) {
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

func newGetter(stores map[string]kv.Provider) resolver.StoreGetter {
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
			wantErr: resolver.ErrInvalidJSON,
		},
		{
			name:    "field not found in JSON payload",
			stores:  map[string]kv.Provider{"vault": &mockProvider{value: `{"password":"secret"}`}},
			input:   "kv://vault/secret#username",
			wantErr: resolver.ErrFieldNotFound,
		},
		{
			name:    "inline token store not found",
			input:   "prefix-$kv{ghost:key}-suffix",
			wantErr: kv.ErrStoreNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := resolver.NewResolver(newGetter(tc.stores), nil)
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

func TestResolve_KeyNotFound(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{err: &kv.KeyNotFoundError{StoreName: "env", KeyPath: "MISSING"}},
	})
	r := resolver.NewResolver(getter, nil)

	_, err := r.Resolve(t.Context(), "kv://env/MISSING")

	var target *kv.KeyNotFoundError
	assert.ErrorAs(t, err, &target)
}

func TestResolve_WholeValue_FragmentNormalization(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: `{"password":"secret"}`},
	})
	r := resolver.NewResolver(getter, nil)

	withoutSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#password")
	assert.NoError(t, err)

	withSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#/password")
	assert.NoError(t, err)

	assert.Equal(t, withoutSlash, withSlash)
	assert.Equal(t, "secret", withSlash)
}

func TestResolve_JSONPointer_ObjectLeaf_ReserializedAsJSON(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"db":{"host":"localhost","port":5432}}`},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://vault/secret#/db")
	assert.NoError(t, err)
	assert.NotEmpty(t, got)
}

// --------------------------------------------------------------------------
// ResolveAll
// --------------------------------------------------------------------------

func TestResolveAll_FlatDocument(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "resolved-value"},
	})
	r := resolver.NewResolver(getter, nil)

	input := []byte(`{"host":"kv://env/HOST","port":"5432"}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "resolved-value", result["host"])
	assert.Equal(t, "5432", result["port"])
}

func TestResolveAll_NestedObjectAndArray(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "deep-value"},
	})
	r := resolver.NewResolver(getter, nil)

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
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "ok"},
	})
	r := resolver.NewResolver(getter, nil)

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
	r := resolver.NewResolver(newGetter(nil), nil)

	input := []byte(`{"host":"localhost","port":5432}`)

	got, err := r.ResolveAll(t.Context(), input)
	assert.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Equal(t, "localhost", result["host"])
	assert.Equal(t, float64(5432), result["port"])
}

func TestResolveAll_EmptyDocument(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	got, err := r.ResolveAll(t.Context(), []byte(`{}`))
	assert.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(got, &result))
	assert.Empty(t, result)
}

func TestResolveAll_OneFieldFails_ErrorReturned(t *testing.T) {
	getter := newGetter(nil) // no stores registered → any kv:// ref fails
	r := resolver.NewResolver(getter, nil)

	input := []byte(`{"a":"kv://missing/key","b":"plain"}`)

	_, err := r.ResolveAll(t.Context(), input)
	assert.ErrorIs(t, err, kv.ErrStoreNotFound)
}

func TestResolveAll_InvalidJSON_Input(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	_, err := r.ResolveAll(t.Context(), []byte(`not json`))
	assert.Error(t, err)
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
	r := resolver.NewResolver(getter, nil)
	input := "postgres://$kv{vault:db/creds#host}:$kv{vault:db/creds#port}/$kv{env:DB_NAME}"
	ctx := context.Background()

	b.ResetTimer()

	for b.Loop() {
		_, _ = r.Resolve(ctx, input)
	}
}
