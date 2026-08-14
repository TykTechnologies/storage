package kv_test

import (
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

// decoder mirrors kelseyhightower/envconfig's Decoder interface. Asserting
// *Stores against it here pins the method shape a loader relies on, without
// making this package depend on envconfig.
type decoder interface {
	Decode(value string) error
}

func TestStores_ImplementsDecoder(t *testing.T) {
	var _ decoder = (*kv.Stores)(nil)
}

func TestStores_Decode_PopulatesFromJSON(t *testing.T) {
	var stores kv.Stores

	err := stores.Decode(`{
		"vault-prod": {
			"type": "hashicorp_vault",
			"required": true,
			"config": {"address": "http://localhost:8200", "namespace": "team-a"}
		}
	}`)
	require.NoError(t, err)

	require.Len(t, stores, 1)
	sc, ok := stores["vault-prod"]
	require.True(t, ok)
	require.Equal(t, kv.Vault, sc.Type)
	require.True(t, sc.Required)
	require.JSONEq(t, `{"address": "http://localhost:8200", "namespace": "team-a"}`, string(sc.Config))
}

func TestStores_Decode_OverridesAndAddsKeepingOthers(t *testing.T) {
	stores := kv.Stores{
		"vault-prod": {Type: kv.Vault, Config: []byte(`{"namespace": "from-file"}`)},
		"local-file": {Type: kv.File, Config: []byte(`{"base_path": "/etc/secrets"}`)},
	}

	err := stores.Decode(`{
		"vault-prod": {"type": "hashicorp_vault", "config": {"namespace": "from-env"}},
		"vault-eu":   {"type": "hashicorp_vault", "config": {"namespace": "eu"}}
	}`)
	require.NoError(t, err)

	require.Len(t, stores, 3, "override + add, untouched entry retained")

	require.JSONEq(t, `{"namespace": "from-env"}`, string(stores["vault-prod"].Config))
	require.Equal(t, kv.Vault, stores["vault-eu"].Type)
	require.Equal(t, kv.File, stores["local-file"].Type)
	require.JSONEq(t, `{"base_path": "/etc/secrets"}`, string(stores["local-file"].Config))
}

func TestStores_Decode_BlankIsNoOp(t *testing.T) {
	stores := kv.Stores{
		"vault-prod": {Type: kv.Vault},
	}

	for _, value := range []string{"", "   ", "\n\t"} {
		require.NoError(t, stores.Decode(value))
		require.Len(t, stores, 1, "blank value %q must not clear existing stores", value)
		require.Equal(t, kv.Vault, stores["vault-prod"].Type)
	}
}

func TestStores_Decode_IntoNilMapAllocates(t *testing.T) {
	var stores kv.Stores
	require.Nil(t, stores)

	require.NoError(t, stores.Decode(`{"env-1": {"type": "env", "config": {"prefix": "TYK_KV_"}}}`))
	require.Len(t, stores, 1)
	require.Equal(t, kv.Env, stores["env-1"].Type)
}

func TestStores_Decode_InvalidJSONErrors(t *testing.T) {
	var stores kv.Stores

	err := stores.Decode(`{"vault-prod": `)
	require.Error(t, err)
}
