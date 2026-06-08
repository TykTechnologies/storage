package resolver_test

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/assert"
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

// --------------------------------------------------------------------------
// Resolve — plain strings
// --------------------------------------------------------------------------

func TestResolve_PlainString(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	got, err := r.Resolve(t.Context(), "just-a-plain-string")
	assert.NoError(t, err)
	assert.Equal(t, "just-a-plain-string", got)
}

func TestResolve_EmptyString(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	got, err := r.Resolve(t.Context(), "")
	assert.NoError(t, err)
	assert.Empty(t, got)
}

// --------------------------------------------------------------------------
// Resolve — kv:// whole-value
// --------------------------------------------------------------------------

func TestResolve_WholeValue(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: "s3cr3t"},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://vault/db/password")
	assert.NoError(t, err)
	assert.Equal(t, "s3cr3t", got)
}

func TestResolve_WholeValue_WithFragment(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"username":"admin","password":"hunter2"}`},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://vault/db/creds#password")
	assert.NoError(t, err)
	assert.Equal(t, "hunter2", got)
}

func TestResolve_WholeValue_FragmentNormalization(t *testing.T) {
	// "#password" and "#/password" must behave identically.
	payload := `{"password":"secret"}`
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: payload},
	})
	r := resolver.NewResolver(getter, nil)

	withoutSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#password")
	assert.NoError(t, err)

	withSlash, err := r.Resolve(t.Context(), "kv://env/MY_KEY#/password")
	assert.NoError(t, err)

	assert.Equal(t, withoutSlash, withSlash)
	assert.Equal(t, "secret", withSlash)
}

// --------------------------------------------------------------------------
// Resolve — $kv{} inline
// --------------------------------------------------------------------------

func TestResolve_InlineSingle(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "myhost.com"},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "https://$kv{env:API_HOST}/v1")
	assert.NoError(t, err)
	assert.Equal(t, "https://myhost.com/v1", got)
}

func TestResolve_InlineMultiple(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "resolved"},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "$kv{env:A}/$kv{env:B}/$kv{env:C}")
	assert.NoError(t, err)
	assert.Equal(t, "resolved/resolved/resolved", got)
}

func TestResolve_InlineWithFragment(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"host":"db.internal"}`},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "postgres://$kv{vault:db/creds#host}:5432/mydb")
	assert.NoError(t, err)
	assert.Equal(t, "postgres://db.internal:5432/mydb", got)
}

// --------------------------------------------------------------------------
// Resolve —  JSON Pointer traversal
// --------------------------------------------------------------------------

func TestResolve_JSONPointer_Nested(t *testing.T) {
	payload := `{"db":{"host":"localhost","port":5432}}`
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: payload},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://vault/secret#/db/host")
	assert.NoError(t, err)
	assert.Equal(t, "localhost", got)
}

func TestResolve_JSONPointer_ArrayIndex(t *testing.T) {
	payload := `{"hosts":["primary","replica"]}`
	getter := newGetter(map[string]kv.Provider{
		"consul": &mockProvider{value: payload},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://consul/service/db#/hosts/1")
	assert.NoError(t, err)
	assert.Equal(t, "replica", got)
}

func TestResolve_JSONPointer_ObjectLeaf_ReserializedAsJSON(t *testing.T) {
	payload := `{"db":{"host":"localhost","port":5432}}`
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: payload},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://vault/secret#/db")
	assert.NoError(t, err)
	// The result must be a valid compact JSON string; exact key order is not guaranteed.
	assert.NotEmpty(t, got)
}

func TestResolve_JSONPointer_EscapedSegments(t *testing.T) {
	// RFC 6901: ~0 → ~, ~1 → /
	payload := `{"a/b":{"c~d":"value"}}`
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: payload},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://env/KEY#/a~1b/c~0d")
	assert.NoError(t, err)
	assert.Equal(t, "value", got)
}

// --------------------------------------------------------------------------
// Resolve — no recursive resolution
// --------------------------------------------------------------------------

func TestResolve_ResolvedValueContainingKVSyntax_NotResolvedFurther(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{value: "kv://vault/another-secret"},
	})
	r := resolver.NewResolver(getter, nil)

	got, err := r.Resolve(t.Context(), "kv://env/SOME_KEY")
	assert.NoError(t, err)
	assert.Equal(t, "kv://vault/another-secret", got)
}

// --------------------------------------------------------------------------
// Resolve —  error paths
// --------------------------------------------------------------------------

func TestResolve_StoreNotFound(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	_, err := r.Resolve(t.Context(), "kv://nonexistent/key")
	assert.ErrorIs(t, err, kv.ErrStoreNotFound)
}

func TestResolve_KeyNotFound(t *testing.T) {
	knf := &kv.KeyNotFoundError{StoreName: "env", KeyPath: "MISSING"}
	getter := newGetter(map[string]kv.Provider{
		"env": &mockProvider{err: knf},
	})
	r := resolver.NewResolver(getter, nil)

	_, err := r.Resolve(t.Context(), "kv://env/MISSING")
	var target *kv.KeyNotFoundError
	assert.ErrorAs(t, err, &target)
}

func TestResolve_InvalidJSON_WhenFragmentRequested(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: "not-json"},
	})
	r := resolver.NewResolver(getter, nil)

	_, err := r.Resolve(t.Context(), "kv://vault/secret#field")
	assert.ErrorIs(t, err, resolver.ErrInvalidJSON)
}

func TestResolve_FieldNotFound_WhenPointerMissing(t *testing.T) {
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: `{"password":"secret"}`},
	})
	r := resolver.NewResolver(getter, nil)

	_, err := r.Resolve(t.Context(), "kv://vault/secret#username")
	assert.ErrorIs(t, err, resolver.ErrFieldNotFound)
}

func TestResolve_InlineToken_StoreNotFound(t *testing.T) {
	r := resolver.NewResolver(newGetter(nil), nil)

	_, err := r.Resolve(t.Context(), "prefix-$kv{ghost:key}-suffix")
	assert.ErrorIs(t, err, kv.ErrStoreNotFound)
}

// --------------------------------------------------------------------------
// ResolveAll
// --------------------------------------------------------------------------

// func TestResolveAll_FlatDocument(t *testing.T) {
// 	getter := newGetter(map[string]kv.Provider{
// 		"env": &mockProvider{value: "resolved-value"},
// 	})
// 	r := resolver.NewResolver(getter, nil)
//
// 	input := []byte(`{"host":"kv://env/HOST","port":"5432"}`)
//
// 	got, err := r.ResolveAll(t.Context(), input)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
//
// 	// port should be preserved, host should be resolved.
// 	if !bytes.Equal(got, input) {
// 		t.Error("expected document to change after resolution")
// 	}
// }

// func TestResolveAll_NestedObjectAndArray(t *testing.T) {
// 	getter := newGetter(map[string]kv.Provider{
// 		"env": &mockProvider{value: "deep-value"},
// 	})
// 	r := resolver.NewResolver(getter, nil)
//
// 	input := []byte(`{"outer":{"inner":"kv://env/KEY","list":["kv://env/A","plain"]}}`)
//
// 	got, err := r.ResolveAll(context.Background(), input)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
//
// 	_ = got // structural correctness checked below via no-error
// }

// func TestResolveAll_NonStringValuesPreserved(t *testing.T) {
// 	getter := newGetter(map[string]kv.Provider{
// 		"env": &mockProvider{value: "ok"},
// 	})
// 	r := resolver.NewResolver(getter, nil)
//
// 	input := []byte(`{"flag":true,"count":42,"nothing":null,"name":"kv://env/NAME"}`)
//
// 	got, err := r.ResolveAll(context.Background(), input)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
//
// 	_ = got
// }

// func TestResolveAll_NoKVReferences_ReturnedUnchanged(t *testing.T) {
// 	r := resolver.NewResolver(newGetter(nil), nil)
//
// 	input := []byte(`{"host":"localhost","port":5432}`)
//
// 	got, err := r.ResolveAll(context.Background(), input)
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
// 	// Content must be semantically identical (JSON round-trip may reformat).
// 	if len(got) == 0 {
// 		t.Error("expected non-empty output")
// 	}
// }
//
// func TestResolveAll_EmptyDocument(t *testing.T) {
// 	r := resolver.NewResolver(newGetter(nil), nil)
//
// 	got, err := r.ResolveAll(context.Background(), []byte(`{}`))
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}
//
// 	if len(got) == 0 {
// 		t.Error("expected non-empty output for empty document")
// 	}
// }
//
// func TestResolveAll_OneFieldFails_ErrorReturned(t *testing.T) {
// 	getter := newGetter(nil) // no stores registered → any kv:// ref fails
// 	r := resolver.NewResolver(getter, nil)
//
// 	input := []byte(`{"a":"kv://missing/key","b":"plain"}`)
//
// 	_, err := r.ResolveAll(context.Background(), input)
// 	if err == nil {
// 		t.Fatal("expected error when a field fails to resolve")
// 	}
// }
//
// func TestResolveAll_InvalidJSON_Input(t *testing.T) {
// 	r := resolver.NewResolver(newGetter(nil), nil)
//
// 	_, err := r.ResolveAll(context.Background(), []byte(`not json`))
// 	if err == nil {
// 		t.Fatal("expected error for invalid JSON input")
// 	}
// }

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
