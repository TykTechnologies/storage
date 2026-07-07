package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockProvider struct{}

func (m *mockProvider) Get(ctx context.Context, path string) (string, error) {
	return "", nil
}

func (m *mockProvider) Unwrap() Provider {
	return m
}

func TestAs_CircularDependencyWontFail(t *testing.T) {
	t.Parallel()

	m := &mockProvider{}
	_, ok := As[Closer](m)

	require.False(t, ok)
}

func TestProviderType_IsLocal(t *testing.T) {
	t.Parallel()

	local := []ProviderType{
		Env,
		Inline,
		File,
	}

	remote := []ProviderType{
		Vault,
		Consul,
		AWS,
		GCP,
		Azure,
		Conjur,
		"unknown_provider",
		"",
	}

	for _, pt := range local {
		require.Truef(t, pt.IsLocal(), "%q must initialize in Phase 1", pt)
	}

	for _, pt := range remote {
		require.Falsef(t, pt.IsLocal(), "%q must NOT be treated as a Phase 1 local store", pt)
	}
}
