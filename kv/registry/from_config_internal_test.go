package registry

import (
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestIsLocalType(t *testing.T) {
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
		require.Truef(t, isLocalType(pt), "%q must initialize in Phase 1", pt)
	}

	for _, pt := range remote {
		require.Falsef(t, isLocalType(pt), "%q must NOT be treated as a Phase 1 local store", pt)
	}
}
