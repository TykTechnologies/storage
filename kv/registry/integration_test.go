package registry_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/env"
	"github.com/TykTechnologies/storage/kv/registry"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func promotedDefaults(t *testing.T, basePath string, secrets map[string]string) map[string]kv.StoreConfig {
	t.Helper()

	envCfg, err := json.Marshal(map[string]any{"prefix": "TYK_SECRET_", "uppercase": true})
	require.NoError(t, err)

	stores := map[string]kv.StoreConfig{
		"env": {Type: kv.Env, Config: envCfg},
	}

	if basePath != "" {
		fileCfg, err := json.Marshal(map[string]any{"base_path": basePath})
		require.NoError(t, err)

		stores["file"] = kv.StoreConfig{Type: kv.File, Config: fileCfg}
	}

	if secrets != nil {
		inlineCfg, err := json.Marshal(map[string]any{"data": secrets})
		require.NoError(t, err)

		stores["secrets"] = kv.StoreConfig{Type: kv.Inline, Config: inlineCfg}
	}

	return stores
}

// TestIntegrationResolveAllLocalProviders resolves a whole config document that
// mixes every local provider, both reference syntaxes, and a JSON-pointer
// fragment — proving the providers cooperate through the resolver exactly as the
// caller expects.
func TestIntegrationResolveAllLocalProviders(t *testing.T) {
	t.Setenv("TYK_SECRET_DB_PASSWORD", "s3cret")

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), []byte("CERTDATA\n"), 0o600))

	secrets := map[string]string{
		"api_key": "inline-key",
		"blob":    `{"user":"admin","pass":"pw"}`,
	}

	reg := newRegistry(t, nil, registry.WithDefaultStores(promotedDefaults(t, dir, secrets)))
	res := resolver.NewResolver(reg)

	doc := []byte(`{
		"db_password": "kv://env/db_password",
		"api_key":     "kv://secrets/api_key",
		"cert":        "kv://file/tls.crt",
		"url":         "https://$kv{secrets:blob#user}.example.com",
		"admin_pass":  "kv://secrets/blob#pass",
		"missing_env": "kv://env/NOT_SET_ANYWHERE"
	}`)

	out, err := res.ResolveAll(t.Context(), doc)
	require.NoError(t, err)

	var got struct {
		DBPassword string `json:"db_password"`
		APIKey     string `json:"api_key"`
		Cert       string `json:"cert"`
		URL        string `json:"url"`
		AdminPass  string `json:"admin_pass"`
		MissingEnv string `json:"missing_env"`
	}
	require.NoError(t, json.Unmarshal(out, &got))

	assert.Equal(t, "s3cret", got.DBPassword, "env: TYK_SECRET_ prefix + uppercase")
	assert.Equal(t, "inline-key", got.APIKey, "inline: whole-value lookup")
	assert.Equal(t, "CERTDATA", got.Cert, "file: read with trailing newline trimmed")
	assert.Equal(t, "https://admin.example.com", got.URL, "inline + $kv{} inline token + #fragment")
	assert.Equal(t, "pw", got.AdminPass, "inline: kv:// whole-value with #fragment")
	assert.Equal(t, "", got.MissingEnv, "env: missing variable resolves to empty, no error")
}

func TestIntegrationInlineMissingKeyIsFatal(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t, nil, registry.WithDefaultStores(
		promotedDefaults(t, "", map[string]string{"present": "v"}),
	))
	res := resolver.NewResolver(reg)

	_, err := res.Resolve(t.Context(), "kv://secrets/absent")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound)
	assert.Equal(t, "absent", notFound.KeyPath)
}

func TestIntegrationEnvMissingKeyIsNotFatal(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t, nil, registry.WithDefaultStores(promotedDefaults(t, "", nil)))
	res := resolver.NewResolver(reg)

	got, err := res.Resolve(t.Context(), "kv://env/UNSET_OPTIONAL_SECRET")
	require.NoError(t, err)
	assert.Equal(t, "", got)
}

func TestIntegrationPhase1ResolvesRemoteTokenFromRealEnv(t *testing.T) {
	t.Setenv("TYK_SECRET_VAULT_TOKEN", "hvs.real")

	doc := []byte(`{
		"kv": {
			"stores": {
				"vault": {"type": "hashicorp_vault", "config": {"token": "kv://env/VAULT_TOKEN"}}
			}
		}
	}`)

	rec := &configRecorder{}
	reg := newRegistry(t, doc,
		registry.WithDefaultStores(promotedDefaults(t, "", nil)),
		registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Vault: recordingFactory(rec, &fakeProvider{}, nil),
		}),
	)

	_, err := reg.GetStore("vault")
	require.NoError(t, err)
	assert.Equal(t, "hvs.real", tokenOf(t, rec.single(t)),
		"real env provider must resolve the vault token in Phase 1")
}

func TestIntegrationEnvEmptyPrefixRejectedThroughStack(t *testing.T) {
	t.Parallel()

	openEnv, err := json.Marshal(map[string]any{"prefix": "", "uppercase": true})
	require.NoError(t, err)

	reg := newRegistry(t, nil, registry.WithDefaultStores(map[string]kv.StoreConfig{
		"openenv": {Type: kv.Env, Config: openEnv},
	}))
	res := resolver.NewResolver(reg)

	_, err = res.Resolve(t.Context(), "kv://openenv/PATH")
	require.ErrorIs(t, err, env.ErrPrefixRequired)
}
