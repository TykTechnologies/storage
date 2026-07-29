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
