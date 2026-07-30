package resolver_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/kv/resolver"
)

func TestValidateSyntax(t *testing.T) {
	t.Parallel()

	t.Run("reference-free string is valid", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, resolver.ValidateSyntax("https://example.com/api"))
	})

	t.Run("well-formed inline token is valid", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, resolver.ValidateSyntax("https://$kv{env:API_HOST}/v1"))
	})

	t.Run("well-formed whole-value reference is valid", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, resolver.ValidateSyntax("kv://vault/secret/data/app#password"))
	})

	t.Run("unclosed inline token wraps ErrMalformedReference", func(t *testing.T) {
		t.Parallel()
		require.ErrorIs(t, resolver.ValidateSyntax("https://example.com/$kv{unclosed"), resolver.ErrMalformedReference)
	})

	t.Run("malformed whole-value reference wraps ErrMalformedReference", func(t *testing.T) {
		t.Parallel()
		require.ErrorIs(t, resolver.ValidateSyntax("kv://no-path-separator"), resolver.ErrMalformedReference)
	})
}

func TestValidateSyntaxAll(t *testing.T) {
	t.Parallel()

	t.Run("document with only well-formed references is valid", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{"upstream":{"url":"https://$kv{env:HOST}/v1"},"secret":"kv://vault/db#pw"}`)
		require.NoError(t, resolver.ValidateSyntaxAll(doc))
	})

	t.Run("malformed reference in a non-URL field wraps ErrMalformedReference", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{"middleware":{"add":[{"name":"X-KV","value":"kv://random-gcpdb-password"}]}}`)
		require.ErrorIs(t, resolver.ValidateSyntaxAll(doc), resolver.ErrMalformedReference)
	})

	t.Run("unparseable document wraps ErrInvalidJSON", func(t *testing.T) {
		t.Parallel()
		require.ErrorIs(t, resolver.ValidateSyntaxAll([]byte(`{"x":"kv://vault/ok"`)), resolver.ErrInvalidJSON)
	})
}
