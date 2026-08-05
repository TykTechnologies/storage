package vault_test

import (
	"context"
	"encoding/json"
	"io"
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
//
// Note on t.Parallel: every test clears the VAULT_* environment via clearVaultEnv
// (t.Setenv) so a developer's shell can't perturb client construction. t.Setenv is
// incompatible with t.Parallel by design, so these tests are intentionally serial
// — the suite runs in well under a second, so there is nothing to gain from
// parallelism anyway.

// clearVaultEnv blanks the VAULT_* environment so client construction is
// hermetic.
func clearVaultEnv(t *testing.T) {
	t.Helper()

	for _, k := range []string{
		"VAULT_ADDR", "VAULT_AGENT_ADDR", "VAULT_TOKEN",
		"VAULT_CACERT", "VAULT_CAPATH", "VAULT_CLIENT_CERT",
		"VAULT_CLIENT_KEY", "VAULT_SKIP_VERIFY", "VAULT_TLS_SERVER_NAME",
		"VAULT_MAX_RETRIES", "VAULT_CLIENT_TIMEOUT", "VAULT_RATE_LIMIT",
		"VAULT_NAMESPACE",
	} {
		t.Setenv(k, "")
	}
}

// vaultStub is an httptest server that records the requests it receives and
// delegates response construction to a per-test handler.
type vaultStub struct {
	url        string
	mu         sync.Mutex
	got        []string
	bodies     []string
	namespaces []string
	nsPresent  []bool
}

// requests returns a copy of the recorded "METHOD /path" entries.
func (s *vaultStub) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.got...)
}

// lastNamespace returns the value of the X-Vault-Namespace header on the most
// recent request. It is "" both when the header was absent and when it was
// present with an empty value — use lastNamespacePresent to tell them apart.
func (s *vaultStub) lastNamespace() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.namespaces) == 0 {
		return ""
	}

	return s.namespaces[len(s.namespaces)-1]
}

func (s *vaultStub) lastNamespacePresent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.nsPresent) == 0 {
		return false
	}

	return s.nsPresent[len(s.nsPresent)-1]
}

// lastBody returns the raw request body of the most recent request.
func (s *vaultStub) lastBody() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.bodies) == 0 {
		return ""
	}

	return s.bodies[len(s.bodies)-1]
}

func newVaultStub(t *testing.T, handler http.HandlerFunc) *vaultStub {
	t.Helper()

	s := &vaultStub{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		s.mu.Lock()
		s.got = append(s.got, r.Method+" "+r.URL.Path)
		s.bodies = append(s.bodies, string(body))
		s.namespaces = append(s.namespaces, r.Header.Get("X-Vault-Namespace"))
		s.nsPresent = append(s.nsPresent, len(r.Header.Values("X-Vault-Namespace")) > 0)
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
		//nolint:errcheck
		_ = json.NewEncoder(w).Encode(body)
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

func setter(t *testing.T, p kv.Provider) kv.Setter {
	t.Helper()

	s, ok := kv.AsSetter(p)
	require.True(t, ok, "vault provider must implement kv.Setter")

	return s
}

func TestNewFactory(t *testing.T) {
	tests := []struct {
		name            string
		config          string
		wantErr         bool
		wantErrContains string
	}{
		{
			name:   "valid full config",
			config: `{"address":"http://vault.test:8200","token":"root","kv_version":2}`,
		},
		{
			name:   "token only (address is optional)",
			config: `{"token":"root"}`,
		},
		{
			name:   "agent_address with token",
			config: `{"agent_address":"http://127.0.0.1:8100","token":"root"}`,
		},
		{
			name:   "namespace is accepted",
			config: `{"token":"root","namespace":"team-a/prod"}`,
		},

		// Vault has no usable zero value: a token is required even in agent mode,
		// so every tokenless config is rejected.
		{
			name:    "empty bytes",
			config:  ``,
			wantErr: true,
		},
		{
			name:    "empty object",
			config:  `{}`,
			wantErr: true,
		},
		{
			name:    "address without token",
			config:  `{"address":"http://vault.test:8200"}`,
			wantErr: true,
		},
		{
			name:    "agent_address without token",
			config:  `{"agent_address":"http://127.0.0.1:8100"}`,
			wantErr: true,
		},
		{
			name:    "address and agent_address without token",
			config:  `{"address":"http://vault.test:8200","agent_address":"http://127.0.0.1:8100"}`,
			wantErr: true,
		},

		// Present-but-invalid config. The factory namespaces these with "vault:".
		{
			name:            "invalid json",
			config:          `{not json`,
			wantErr:         true,
			wantErrContains: "vault",
		},
		{
			name:            "invalid timeout (not a duration string)",
			config:          `{"token":"root","timeout":"5x"}`,
			wantErr:         true,
			wantErrContains: "vault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearVaultEnv(t)

			p, err := vault.NewFactory()(json.RawMessage(tt.config))

			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, p)

				if tt.wantErrContains != "" {
					require.ErrorContains(t, err, tt.wantErrContains)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
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

func TestProvider_IsNotStandaloner(t *testing.T) {
	p := newVaultProvider(t, &vault.Config{Token: "root"})

	// Vault is remote and must be wrapped in the registry's cache/singleflight
	// decorator, so it must NOT report itself standaloner.
	s, ok := kv.AsStandaloner(p)
	require.False(t, ok && s.IsStandalone(),
		"vault must not be standalone (the registry must wrap it in the cache)")
}

func TestGet_ReadsSecret(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		kvVersion int
		secret    map[string]any
		wantPath  string
		wantJSON  string
	}{
		{
			name:      "kv2 multi-segment path returns the whole data map",
			key:       "secret/myapp/config",
			kvVersion: 2,
			secret:    map[string]any{"api_key": "abc123", "username": "bob"},
			wantPath:  "GET /v1/secret/data/myapp/config",
			wantJSON:  `{"api_key":"abc123","username":"bob"}`,
		},
		{
			name:      "kv2 default version (0) injects /data",
			key:       "secret/myapp/config",
			kvVersion: 0,
			secret:    map[string]any{"api_key": "abc123"},
			wantPath:  "GET /v1/secret/data/myapp/config",
			wantJSON:  `{"api_key":"abc123"}`,
		},
		{
			name:      "kv2 single-segment path injects /data after the mount",
			key:       "mysecret",
			kvVersion: 2,
			secret:    map[string]any{"api_key": "abc123"},
			wantPath:  "GET /v1/mysecret/data",
			wantJSON:  `{"api_key":"abc123"}`,
		},
		{
			name:      "kv1 reads the path as-is with no unwrap",
			key:       "secret/myapp",
			kvVersion: 1,
			secret:    map[string]any{"api_key": "abc123"},
			wantPath:  "GET /v1/secret/myapp",
			wantJSON:  `{"api_key":"abc123"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
				if tt.kvVersion == 1 {
					writeJSON(w, http.StatusOK, kvv1Envelope(tt.secret))
					return
				}

				writeJSON(w, http.StatusOK, kvv2Envelope(tt.secret))
			})

			p := newVaultProvider(t, &vault.Config{
				Address:   stub.url,
				Token:     "root",
				KVVersion: tt.kvVersion,
			})

			got, err := p.Get(t.Context(), tt.key)
			require.NoError(t, err)
			require.JSONEq(t, tt.wantJSON, got)
			require.Equal(t, []string{tt.wantPath}, stub.requests())
		})
	}
}

func TestGet_MountPath(t *testing.T) {
	tests := []struct {
		name      string
		mountPath string
		key       string
		kvVersion int
		wantPath  string
		wantErr   bool
	}{
		{
			name:      "kv2 nested mount injects /data after the configured mount",
			mountPath: "tenants/a/kv",
			key:       "tenants/a/kv/myapp/config",
			kvVersion: 2,
			wantPath:  "GET /v1/tenants/a/kv/data/myapp/config",
		},
		{
			name:      "kv2 single-segment mount matches the legacy result",
			mountPath: "secret",
			key:       "secret/myapp",
			kvVersion: 2,
			wantPath:  "GET /v1/secret/data/myapp",
		},
		{
			name:      "kv2 trailing slash on mount_path is normalized",
			mountPath: "tenants/a/kv/",
			key:       "tenants/a/kv/myapp",
			kvVersion: 2,
			wantPath:  "GET /v1/tenants/a/kv/data/myapp",
		},
		{
			name:      "kv1 ignores mount_path and reads the path as-is",
			mountPath: "secret",
			key:       "secret/myapp",
			kvVersion: 1,
			wantPath:  "GET /v1/secret/myapp",
		},
		{
			name:      "kv2 key outside the mount is rejected before any request",
			mountPath: "tenants/a/kv",
			key:       "other/secret",
			kvVersion: 2,
			wantErr:   true,
		},
		{
			name:      "kv2 key sharing a string prefix but not under the mount is rejected",
			mountPath: "secret",
			key:       "secrets/myapp",
			kvVersion: 2,
			wantErr:   true,
		},
		{
			name:      "kv2 key using ../ to escape the mount is rejected",
			mountPath: "tenants/a/kv",
			key:       "tenants/a/kv/../../b/kv/secret",
			kvVersion: 2,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
				if tt.kvVersion == 1 {
					writeJSON(w, http.StatusOK, kvv1Envelope(map[string]any{"k": "v"}))
					return
				}

				writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"k": "v"}))
			})

			p := newVaultProvider(t, &vault.Config{
				Address:   stub.url,
				Token:     "root",
				KVVersion: tt.kvVersion,
				MountPath: tt.mountPath,
			})

			_, err := p.Get(t.Context(), tt.key)

			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, stub.requests(),
					"a key outside mount_path must be rejected before any Vault request")

				return
			}

			require.NoError(t, err)
			require.Equal(t, []string{tt.wantPath}, stub.requests())
		})
	}
}

func TestNamespace(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		wantNamespace string
	}{
		{
			name:          "single-segment namespace is sent as-is",
			namespace:     "team-a",
			wantNamespace: "team-a",
		},
		{
			name:          "nested namespace is sent as-is",
			namespace:     "team-a/prod",
			wantNamespace: "team-a/prod",
		},
		{
			name:          "surrounding slashes and whitespace are trimmed",
			namespace:     "  /team-a/prod/  ",
			wantNamespace: "team-a/prod",
		},
		{
			name:          "whitespace interleaved with bounding slashes is trimmed",
			namespace:     "/ team-a/prod /",
			wantNamespace: "team-a/prod",
		},
		{
			name:          "empty namespace sends no header",
			namespace:     "",
			wantNamespace: "",
		},
		{
			name:          "slash/whitespace-only namespace is treated as unset",
			namespace:     "  /  ",
			wantNamespace: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("Get", func(t *testing.T) {
				stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"k": "v"}))
				})

				p := newVaultProvider(t, &vault.Config{
					Address:   stub.url,
					Token:     "root",
					KVVersion: 2,
					Namespace: tt.namespace,
				})

				_, err := p.Get(t.Context(), "secret/myapp/config")
				require.NoError(t, err)

				require.Equal(t, tt.wantNamespace, stub.lastNamespace())
				// A non-empty namespace must put a header on the wire; an unset one
				// must send NO header at all (not an empty-valued one).
				require.Equal(t, tt.wantNamespace != "", stub.lastNamespacePresent())
				// The namespace lives in the header, not the path: path is unchanged.
				require.Equal(t, []string{"GET /v1/secret/data/myapp/config"}, stub.requests())
			})

			t.Run("Set", func(t *testing.T) {
				stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{})
				})

				p := newVaultProvider(t, &vault.Config{
					Address:   stub.url,
					Token:     "root",
					KVVersion: 2,
					Namespace: tt.namespace,
				})

				err := setter(t, p).Set(t.Context(), "secret/myapp/config", `{"k":"v"}`)
				require.NoError(t, err)

				require.Equal(t, tt.wantNamespace, stub.lastNamespace())
				require.Equal(t, tt.wantNamespace != "", stub.lastNamespacePresent())
				require.Equal(t, []string{"PUT /v1/secret/data/myapp/config"}, stub.requests())
			})
		})
	}
}

func TestNamespace_FromEnvironment(t *testing.T) {
	tests := []struct {
		name          string
		envNamespace  string
		cfgNamespace  string
		wantNamespace string
	}{
		{
			name:          "VAULT_NAMESPACE is honored when no namespace is configured",
			envNamespace:  "team-env",
			cfgNamespace:  "",
			wantNamespace: "team-env",
		},
		{
			name:          "explicit config namespace overrides VAULT_NAMESPACE",
			envNamespace:  "team-env",
			cfgNamespace:  "team-cfg",
			wantNamespace: "team-cfg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"k": "v"}))
			})

			clearVaultEnv(t)
			t.Setenv("VAULT_NAMESPACE", tt.envNamespace)

			raw, err := json.Marshal(&vault.Config{
				Address:   stub.url,
				Token:     "root",
				KVVersion: 2,
				Namespace: tt.cfgNamespace,
			})
			require.NoError(t, err)

			p, err := vault.NewFactory()(raw)
			require.NoError(t, err)

			_, err = p.Get(t.Context(), "secret/myapp/config")
			require.NoError(t, err)

			require.Equal(t, tt.wantNamespace, stub.lastNamespace())
			require.Equal(t, []string{"GET /v1/secret/data/myapp/config"}, stub.requests())
		})
	}
}

func TestNamespace_JSONConfigKey(t *testing.T) {
	clearVaultEnv(t)

	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, kvv2Envelope(map[string]any{"k": "v"}))
	})

	raw := json.RawMessage(
		`{"address":"` + stub.url + `","token":"root","kv_version":2,"namespace":"team-a/prod"}`,
	)

	p, err := vault.NewFactory()(raw)
	require.NoError(t, err)

	_, err = p.Get(t.Context(), "secret/myapp/config")
	require.NoError(t, err)

	require.True(t, stub.lastNamespacePresent(), "the \"namespace\" JSON key must reach the wire as a header")
	require.Equal(t, "team-a/prod", stub.lastNamespace())
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
			t.Error(err)
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

func TestProvider_ImplementsSetter(t *testing.T) {
	p := newVaultProvider(t, &vault.Config{Token: "root"})

	_, ok := kv.AsSetter(p)
	require.True(t, ok, "vault must implement kv.Setter for the write-back path")
}

func TestSet_WritesDataMap(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		kvVersion int
		value     string
		wantPath  string
		wantBody  string
	}{
		{
			name:      "kv2 multi-segment path injects /data and wraps in data envelope",
			key:       "secret/tyk-apis",
			kvVersion: 2,
			value:     `{"api_key":"NEW"}`,
			wantPath:  "PUT /v1/secret/data/tyk-apis",
			wantBody:  `{"data":{"api_key":"NEW"}}`,
		},
		{
			name:      "kv2 default version (0) wraps in data envelope",
			key:       "secret/tyk-apis",
			kvVersion: 0,
			value:     `{"api_key":"NEW"}`,
			wantPath:  "PUT /v1/secret/data/tyk-apis",
			wantBody:  `{"data":{"api_key":"NEW"}}`,
		},
		{
			name:      "kv2 single-segment path injects /data after the mount",
			key:       "mysecret",
			kvVersion: 2,
			value:     `{"api_key":"NEW"}`,
			wantPath:  "PUT /v1/mysecret/data",
			wantBody:  `{"data":{"api_key":"NEW"}}`,
		},
		{
			name:      "kv2 writes the whole data map faithfully (symmetric with Get: a real C2 secret has many fields)",
			key:       "secret/tyk-apis",
			kvVersion: 2,
			value:     `{"api_key":"NEW","username":"bob"}`,
			wantPath:  "PUT /v1/secret/data/tyk-apis",
			wantBody:  `{"data":{"api_key":"NEW","username":"bob"}}`,
		},
		{
			name:      "kv1 writes the data map as-is with no data envelope",
			key:       "secret/tyk-apis",
			kvVersion: 1,
			value:     `{"api_key":"NEW"}`,
			wantPath:  "PUT /v1/secret/tyk-apis",
			wantBody:  `{"api_key":"NEW"}`,
		},
		{
			name:      "kv2 non-string field values (number, bool) are written faithfully",
			key:       "secret/tyk-apis",
			kvVersion: 2,
			value:     `{"api_key":"NEW","port":8080,"enabled":true}`,
			wantPath:  "PUT /v1/secret/data/tyk-apis",
			wantBody:  `{"data":{"api_key":"NEW","port":8080,"enabled":true}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, map[string]any{})
			})

			p := newVaultProvider(t, &vault.Config{
				Address:   stub.url,
				Token:     "root",
				KVVersion: tt.kvVersion,
			})

			err := setter(t, p).Set(t.Context(), tt.key, tt.value)
			require.NoError(t, err)

			require.Equal(t, []string{tt.wantPath}, stub.requests())
			require.JSONEq(t, tt.wantBody, stub.lastBody())
		})
	}
}

func TestSet_MountPath(t *testing.T) {
	tests := []struct {
		name      string
		mountPath string
		key       string
		wantPath  string
		wantErr   bool
	}{
		{
			name:      "kv2 nested mount injects /data after the configured mount",
			mountPath: "tenants/a/kv",
			key:       "tenants/a/kv/tyk-apis",
			wantPath:  "PUT /v1/tenants/a/kv/data/tyk-apis",
		},
		{
			name:      "kv2 key outside the mount is rejected before any request",
			mountPath: "tenants/a/kv",
			key:       "other/secret",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, map[string]any{})
			})

			p := newVaultProvider(t, &vault.Config{
				Address:   stub.url,
				Token:     "root",
				KVVersion: 2,
				MountPath: tt.mountPath,
			})

			err := setter(t, p).Set(t.Context(), tt.key, `{"api_key":"NEW"}`)

			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, stub.requests(),
					"a key outside mount_path must be rejected before any Vault request")

				return
			}

			require.NoError(t, err)
			require.Equal(t, []string{tt.wantPath}, stub.requests())
			require.JSONEq(t, `{"data":{"api_key":"NEW"}}`, stub.lastBody())
		})
	}
}

func TestSet_InvalidValueRejectedBeforeRequest(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "not json at all", value: "just a string"},
		{name: "json string scalar is not an object", value: `"api_key"`},
		{name: "json number is not an object", value: "42"},
		{name: "json array is not an object", value: `["a","b"]`},
		{name: "json null is not an object", value: "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newVaultStub(t, func(_ http.ResponseWriter, _ *http.Request) {
				t.Error("Set must not reach Vault when the value is not a JSON object")
			})

			p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

			err := setter(t, p).Set(t.Context(), "secret/tyk-apis", tt.value)
			require.Error(t, err, "value must be a JSON object (the secret data map)")
			require.Empty(t, stub.requests())
		})
	}
}

func TestSet_BackendErrorReturnsStoreUnavailable(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// A 200 with an unparseable body fails fast (no 5xx retry backoff) and
		// deterministically exercises the write error branch.
		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte("{ this is not valid vault json"))
		if err != nil {
			t.Error(err)
		}
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	err := setter(t, p).Set(t.Context(), "secret/tyk-apis", `{"api_key":"NEW"}`)

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable,
		"a backend write failure must map to *kv.StoreUnavailableError")
}

func TestSet_PropagatesContextCancellation(t *testing.T) {
	stub := newVaultStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := setter(t, p).Set(ctx, "secret/tyk-apis", `{"api_key":"NEW"}`)
	require.ErrorIs(t, err, context.Canceled,
		"a cancelled context must abort the write (WriteWithContext)")
}

func TestSet_RoundTripsWithGet(t *testing.T) {
	var (
		mu     sync.Mutex
		stored map[string]any
	)

	var stub *vaultStub
	stub = newVaultStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			data := stored
			mu.Unlock()

			writeJSON(w, http.StatusOK, kvv2Envelope(data))

			return
		}

		var put struct {
			Data map[string]any `json:"data"`
		}
		err := json.Unmarshal([]byte(stub.lastBody()), &put)
		require.NoError(t, err)

		mu.Lock()
		stored = put.Data
		mu.Unlock()

		writeJSON(w, http.StatusOK, map[string]any{})
	})

	p := newVaultProvider(t, &vault.Config{Address: stub.url, Token: "root", KVVersion: 2})

	require.NoError(t, setter(t, p).Set(t.Context(), "secret/tyk-apis", `{"api_key":"NEW","username":"bob"}`))

	got, err := p.Get(t.Context(), "secret/tyk-apis")
	require.NoError(t, err)
	require.JSONEq(t, `{"api_key":"NEW","username":"bob"}`, got,
		"Set then Get must round-trip the data map")
}
