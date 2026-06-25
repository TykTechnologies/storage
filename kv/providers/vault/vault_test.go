package vault_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/vault"
	"github.com/TykTechnologies/storage/kv/registry"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/require"
)

// How these tests work:
//
// The provider is exercised through the real github.com/hashicorp/vault/api
// client against an in-process HTTP server (vaultStub) that stands in for Vault.
// Each test points Config.Address (or AgentAddress) at the stub's URL, so the
// client builds genuine Vault requests and the stub returns canned KV responses.
//
// This means the tests cover the actual client wiring — URL/path construction,
// the KV v2 "/data" injection (asserted via the request path the stub records),
// token handling, and response parsing — rather than a mock of it, while staying
// hermetic and millisecond-fast.

// clearVaultEnv blanks the VAULT_* environment so client construction is
// hermetic.
func clearVaultEnv(t *testing.T) {
	t.Helper()

	for _, k := range []string{
		"VAULT_ADDR", "VAULT_AGENT_ADDR", "VAULT_TOKEN",
		"VAULT_CACERT", "VAULT_CAPATH", "VAULT_CLIENT_CERT",
		"VAULT_CLIENT_KEY", "VAULT_SKIP_VERIFY", "VAULT_TLS_SERVER_NAME",
		"VAULT_MAX_RETRIES", "VAULT_CLIENT_TIMEOUT", "VAULT_RATE_LIMIT",
	} {
		t.Setenv(k, "")
	}
}

// vaultStub is an httptest server that records the requests it receives and
// delegates response construction to a per-test handler.
type vaultStub struct {
	url string
	mu  sync.Mutex
	got []string
}

// requests returns a copy of the recorded "METHOD /path" entries.
func (s *vaultStub) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.got...)
}

func newVaultStub(t *testing.T, handler http.HandlerFunc) *vaultStub {
	t.Helper()

	s := &vaultStub{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.got = append(s.got, r.Method+" "+r.URL.Path)
		s.mu.Unlock()

		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	s.url = srv.URL

	return s
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if body != nil {
		err := json.NewEncoder(w).Encode(body)
		fmt.Println(err)
	}
}

// kvv2Envelope wraps secret data the way Vault's KV v2 engine does on the wire:
// {"data": {"data": <secret>, "metadata": {...}}}.
func kvv2Envelope(data map[string]any) map[string]any {
	return map[string]any{
		"data": map[string]any{
			"data":     data,
			"metadata": map[string]any{"version": 1},
		},
	}
}

// kvv1Envelope wraps secret data the way Vault's KV v1 engine does:
// {"data": <secret>}.
func kvv1Envelope(data map[string]any) map[string]any {
	return map[string]any{"data": data}
}

// newVaultProvider builds the provider through its factory, exactly as the
// registry would, with a hermetic environment.
func newVaultProvider(t *testing.T, cfg *vault.Config) kv.Provider {
	t.Helper()

	clearVaultEnv(t)

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	p, err := vault.NewFactory()(raw)
	require.NoError(t, err)
	require.NotNil(t, p)

	return p
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return b
}

func TestNewFactory_ValidConfigBuildsProvider(t *testing.T) {
	clearVaultEnv(t)

	p, err := vault.NewFactory()(json.RawMessage(
		`{"address":"http://vault.test:8200","token":"root","kv_version":2}`,
	))
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewFactory_RejectsEmptyConfig(t *testing.T) {
	clearVaultEnv(t)

	// Unlike env/file/inline, vault has no usable zero value: without a token it
	// cannot authenticate (matching legacy newVault), so the factory rejects
	// empty/absent config rather than building a disabled provider.
	for _, cfg := range []json.RawMessage{nil, {}, json.RawMessage(`{}`)} {
		p, err := vault.NewFactory()(cfg)
		require.Error(t, err, "config %q", string(cfg))
		require.Nil(t, p, "config %q", string(cfg))
	}
}

func TestNewFactory_RejectsMissingToken(t *testing.T) {
	clearVaultEnv(t)

	for _, cfg := range []json.RawMessage{
		json.RawMessage(`{"address":"http://vault.test:8200"}`),
		json.RawMessage(`{"agent_address":"http://127.0.0.1:8100"}`),
		json.RawMessage(`{"address":"http://vault.test:8200","agent_address":"http://127.0.0.1:8100"}`),
	} {
		p, err := vault.NewFactory()(cfg)
		require.Error(t, err, "config %q", string(cfg))
		require.Nil(t, p, "config %q", string(cfg))
	}
}

func TestNewFactory_AgentAddressWithTokenBuilds(t *testing.T) {
	clearVaultEnv(t)

	p, err := vault.NewFactory()(json.RawMessage(
		`{"agent_address":"http://127.0.0.1:8100","token":"root"}`,
	))
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewFactory_TokenWithoutAddressBuilds(t *testing.T) {
	clearVaultEnv(t)

	p, err := vault.NewFactory()(json.RawMessage(`{"token":"root"}`))
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewFactory_RejectsInvalidJSON(t *testing.T) {
	clearVaultEnv(t)

	p, err := vault.NewFactory()(json.RawMessage(`{not json`))
	require.Error(t, err)
	require.Nil(t, p)
	require.ErrorContains(t, err, "vault", "factory must namespace config errors with a vault: prefix")
}

func TestNewFactory_RejectsInvalidTimeout(t *testing.T) {
	clearVaultEnv(t)

	// timeout is a Go duration string; an unparseable value is present-but-invalid
	// config and must fail at construction.
	p, err := vault.NewFactory()(json.RawMessage(`{"token":"root","timeout":"5x"}`))
	require.Error(t, err)
	require.Nil(t, p)
}

func TestProvider_ReportsConfiguredTimeout(t *testing.T) {
	p := newVaultProvider(t, &vault.Config{Token: "root", Timeout: "7s"})

	to, ok := kv.AsTimeouter(p)
	require.True(t, ok, "vault must expose Timeouter so the registry can bound its operations")
	require.Equal(t, 7*time.Second, to.Timeout())
}

func TestProvider_TimeoutUnsetReportsZero(t *testing.T) {
	p := newVaultProvider(t, &vault.Config{Token: "root"})

	to, ok := kv.AsTimeouter(p)
	require.True(t, ok)
	require.Zero(t, to.Timeout(), "an unset timeout must report 0 so the store applies its own default")
}

func TestProvider_IsNotStandalone(t *testing.T) {
	p := newVaultProvider(t, &vault.Config{Token: "root"})

	// Vault is remote and must be wrapped in the registry's cache/singleflight
	// decorator, so it must NOT report itself standalone.
	s, ok := kv.AsStandalone(p)
	require.False(t, ok && s.IsStandalone(),
		"vault must not be standalone (the registry must wrap it in the cache)")
}

func TestGet_KVv2InjectsDataAndReturnsInnerSecretAsJSON(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{
			"api_key":  "abc123",
			"username": "bob",
		}))
	})

	p := newVaultProvider(t, &vault.Config{
		Address:   stub.url,
		Token:     "root",
		KVVersion: 2,
	})

	got, err := p.Get(t.Context(), "secret/myapp/config")
	require.NoError(t, err)
	// The whole secret data map is returned as JSON (field selection is the
	// resolver's job); the KVv2 metadata envelope is unwrapped and excluded.
	require.JSONEq(t, `{"api_key":"abc123","username":"bob"}`, got)
	// /data is injected after the first path segment.
	require.Equal(t, []string{"GET /v1/secret/data/myapp/config"}, stub.requests())
}

func TestGet_ReturnsCompactJSONWithoutTrailingNewline(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"api_key": "abc123"}))
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	got, err := p.Get(t.Context(), "secret/myapp/config")
	require.NoError(t, err)
	require.Equal(t, `{"api_key":"abc123"}`, got)
}

func TestGet_KVv2DefaultsWhenVersionUnset(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"api_key": "abc123"}))
	})

	// KVVersion omitted (0) must behave as v2 — i.e. /data is injected.
	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root"})

	_, err := p.Get(t.Context(), "secret/myapp/config")
	require.NoError(t, err)
	require.Equal(t, []string{"GET /v1/secret/data/myapp/config"}, stub.requests())
}

func TestGet_KVv2SingleSegmentPathInjection(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"api_key": "abc123"}))
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	_, err := p.Get(t.Context(), "mysecret")
	require.NoError(t, err)
	require.Equal(t, []string{"GET /v1/mysecret/data"}, stub.requests())
}

func TestGet_KVv1UsesPathAsIs(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv1Envelope(map[string]any{"api_key": "abc123"}))
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 1})

	got, err := p.Get(t.Context(), "secret/myapp")
	require.NoError(t, err)
	require.JSONEq(t, `{"api_key":"abc123"}`, got)
	// No /data injection for KV v1.
	require.Equal(t, []string{"GET /v1/secret/myapp"}, stub.requests())
}

func TestGet_SecretNotFoundReturnsKeyNotFound(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// Empty-body 404: vault's client returns (nil, nil) for a missing path.
		w.WriteHeader(http.StatusNotFound)
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	_, err := p.Get(t.Context(), "secret/missing")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound,
		"a missing secret must map to *kv.KeyNotFoundError for negative_ttl_not_found caching")
}

func TestGet_KVv2MissingDataEnvelopeReturnsKeyNotFound(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// 200, but the inner "data" key is absent (only metadata) → the KVv2
		// data.data unwrap fails.
		writeJSON(w, http.StatusOK, map[string]any{
			"data": map[string]any{"metadata": map[string]any{"version": 1}},
		})
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	_, err := p.Get(t.Context(), "secret/myapp/config")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound)
}

func TestGet_BackendErrorReturnsStoreUnavailable(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// A 200 is not retried by the vault client, so an unparseable body fails
		// fast and deterministically exercises the transport/parse error branch
		// (avoids the multi-second retry backoff a 5xx would incur).
		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte("{ this is not valid vault json"))
		if err != nil {
			t.Fatal(err)
		}
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	_, err := p.Get(t.Context(), "secret/myapp/config")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable,
		"a backend failure must map to *kv.StoreUnavailableError for negative_ttl_transient caching")
}

func TestGet_PropagatesContextCancellation(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"api_key": "abc123"}))
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := p.Get(ctx, "secret/myapp/config")
	require.ErrorIs(t, err, context.Canceled)
}

func TestGet_AgentModeRoutesToAgentAddress(t *testing.T) {
	agent := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"api_key": "abc123"}))
	})

	// agent_address (with a token, per the legacy contract) must route requests
	// to the agent, not the default server address.
	p := newVaultProvider(t, &vault.Config{AgentAddress: agent.url, Token: "root", KVVersion: 2})

	got, err := p.Get(t.Context(), "secret/myapp/config")
	require.NoError(t, err)
	require.JSONEq(t, `{"api_key":"abc123"}`, got)
	require.NotEmpty(t, agent.requests(), "the request must be routed to the Vault agent address")
}

func TestResolver_ExtractsFieldFromVaultSecret(t *testing.T) {
	clearVaultEnv(t)

	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{
			"api_key":  "abc123",
			"password": "s3cr3t",
		}))
	})

	r := registry.NewRegistry()
	require.NoError(t, r.Add(kv.Vault, vault.NewFactory()))

	ctx := context.Background()
	require.NoError(t, r.InitStores(ctx, &kv.Config{
		Stores: map[string]kv.StoreConfig{
			"vault": {
				Type:   kv.Vault,
				Config: mustJSON(t, vault.Config{Address: stub.url, Token: "root", KVVersion: 2}),
			},
		},
	}))
	t.Cleanup(func() { _ = r.Close(ctx) })

	res := resolver.NewResolver(r)

	// End-to-end: the provider returns the whole secret as JSON, the resolver
	// extracts the #field via JSON pointer. This pins the core contract change
	// (field extraction lives in the resolver, not the provider).
	got, err := res.Resolve(ctx, "kv://vault/secret/myapp/config#api_key")
	require.NoError(t, err)
	require.Equal(t, "abc123", got)
}
