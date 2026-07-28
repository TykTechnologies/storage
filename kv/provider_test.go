package kv_test

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

type mockProvider struct{}

func (m *mockProvider) Get(ctx context.Context, path string) (string, error) {
	return "", nil
}

func (m *mockProvider) Unwrap() kv.Provider {
	return m
}

func TestAs_CircularDependencyWontFail(t *testing.T) {
	t.Parallel()

	m := &mockProvider{}
	_, ok := kv.As[kv.Closer](m)

	require.False(t, ok)
}

func TestProviderType_IsLocal(t *testing.T) {
	t.Parallel()

	local := []kv.ProviderType{
		kv.Env,
		kv.Inline,
		kv.File,
	}

	remote := []kv.ProviderType{
		kv.Vault,
		kv.Consul,
		kv.AWS,
		kv.GCP,
		kv.Azure,
		kv.Conjur,
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
