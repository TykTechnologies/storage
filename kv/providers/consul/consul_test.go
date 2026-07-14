package consul_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/consul"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// How these tests work:
//
// The provider is exercised through the real github.com/hashicorp/consul/api
// client against an in-process HTTP server (consulStub) that mimics consul's KV
// HTTP API. Each test points Config.Address at the stub, so the client builds
// genuine consul requests and the stub returns canned KV responses. This covers
// the actual client wiring — URL/path construction, base64 value decoding,
// 404→not-found, basic-auth headers — rather than a mock of it, while staying
// hermetic and millisecond-fast.
//

// clearConsulEnv blanks the CONSUL_* environment that consulapi.DefaultConfig
// reads, so client construction is hermetic and reproducible.
func clearConsulEnv(t *testing.T) {
	t.Helper()

	for _, k := range []string{
		"CONSUL_HTTP_ADDR", "CONSUL_HTTP_TOKEN", "CONSUL_HTTP_TOKEN_FILE",
		"CONSUL_HTTP_AUTH", "CONSUL_HTTP_SSL", "CONSUL_HTTP_SSL_VERIFY",
		"CONSUL_CACERT", "CONSUL_CAPATH", "CONSUL_CLIENT_CERT",
		"CONSUL_CLIENT_KEY", "CONSUL_TLS_SERVER_NAME", "CONSUL_NAMESPACE",
	} {
		t.Setenv(k, "")
	}
}

// consulStub is an httptest server that records the requests it receives and
// delegates response construction to a per-test handler.
type consulStub struct {
	url    string
	mu     sync.Mutex
	got    []string
	auth   []string
	bodies []string
}

// requests returns a copy of the recorded "METHOD /path" entries.
func (s *consulStub) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.got...)
}

// lastAuth returns the Authorization header of the most recent request.
func (s *consulStub) lastAuth() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.auth) == 0 {
		return ""
	}

	return s.auth[len(s.auth)-1]
}

// lastBody returns the raw request body of the most recent request. It is
// recorded centrally (under the mutex) so write tests can assert on it without
// racing the server goroutine.
func (s *consulStub) lastBody() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.bodies) == 0 {
		return ""
	}

	return s.bodies[len(s.bodies)-1]
}

func newConsulStub(t *testing.T, handler http.HandlerFunc) *consulStub {
	t.Helper()

	s := &consulStub{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		s.mu.Lock()
		s.got = append(s.got, r.Method+" "+r.URL.Path)
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		s.bodies = append(s.bodies, string(body))
		s.mu.Unlock()

		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	s.url = srv.URL

	return s
}

// writeConsulValue writes the wire shape consul's KV API returns for a present
// key: a JSON array of one KVPair whose Value is base64-encoded. Go marshals a
// []byte field to base64, exactly as consul does and consulapi expects.
func writeConsulValue(w http.ResponseWriter, key, value string) {
	w.Header().Set("Content-Type", "application/json")

	//nolint:errcheck
	_ = json.NewEncoder(w).Encode([]struct {
		Key   string `json:"Key"`
		Value []byte `json:"Value"`
	}{
		{Key: key, Value: []byte(value)},
	})
}

// addrOf strips the scheme so the stub URL is usable as a consul Config.Address
// (host:port), matching how the gateway promotes "consul.internal:8500".
func addrOf(url string) string {
	return url[len("http://"):]
}

// newConsulProvider builds the provider through its factory, exactly as the
// registry would, with a hermetic environment.
func newConsulProvider(t *testing.T, cfg *consul.Config) kv.Provider {
	t.Helper()

	clearConsulEnv(t)

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	p, err := consul.NewFactory()(raw)
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

func writeConsulPairs(w http.ResponseWriter, pairs []struct{ Key, Value string }) {
	w.Header().Set("Content-Type", "application/json")

	type kvp struct {
		Key   string `json:"Key"`
		Value []byte `json:"Value"`
	}

	arr := make([]kvp, 0, len(pairs))
	for _, p := range pairs {
		arr = append(arr, kvp{Key: p.Key, Value: []byte(p.Value)})
	}

	//nolint:errcheck
	_ = json.NewEncoder(w).Encode(arr)
}

func lister(t *testing.T, p kv.Provider) kv.Lister {
	t.Helper()

	l, ok := kv.AsLister(p)
	require.True(t, ok, "consul provider must implement kv.Lister")

	return l
}

func setter(t *testing.T, p kv.Provider) kv.Setter {
	t.Helper()

	s, ok := kv.AsSetter(p)
	require.True(t, ok, "consul provider must implement kv.Setter")

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
			config: `{"address":"127.0.0.1:8500","scheme":"http","datacenter":"dc1","token":"root","wait_time":"5s"}`,
		},
		{
			name:   "address only",
			config: `{"address":"consul.internal:8500"}`,
		},
		{
			name:   "empty object is permissive",
			config: `{}`,
		},
		{
			name:   "empty bytes is permissive",
			config: ``,
		},
		{
			name:   "nil is permissive",
			config: "\x00null-sentinel", // replaced with nil below
		},

		{
			name:   "wait_time 0s (unset duration) is accepted",
			config: `{"wait_time":"0s"}`,
		},
		{
			name:            "invalid json",
			config:          `{not json`,
			wantErr:         true,
			wantErrContains: "consul",
		},
		{
			name:            "invalid wait_time (not a duration string)",
			config:          `{"wait_time":"5x"}`,
			wantErr:         true,
			wantErrContains: "consul",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConsulEnv(t)

			raw := json.RawMessage(tt.config)
			if tt.config == "\x00null-sentinel" {
				raw = nil
			}

			p, err := consul.NewFactory()(raw)

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

func TestNewFactory_InvalidTLSCAFileErrors(t *testing.T) {
	clearConsulEnv(t)

	var cfg consul.Config
	cfg.Address = "127.0.0.1:8500"
	cfg.TLSConfig.CAFile = "/nonexistent/ca-does-not-exist.pem"

	p, err := consul.NewFactory()(mustJSON(t, cfg))
	require.Error(t, err)
	require.Nil(t, p)
	require.ErrorContains(t, err, "consul")
}

func TestProvider_IsNotStandalone(t *testing.T) {
	p := newConsulProvider(t, &consul.Config{})

	s, ok := kv.AsStandalone(p)
	require.False(t, ok && s.IsStandalone(),
		"consul must not be standalone (the registry must wrap it in the cache)")
}

func TestProvider_DoesNotExposeTimeouter(t *testing.T) {
	p := newConsulProvider(t, &consul.Config{WaitTime: "30s"})

	_, ok := kv.AsTimeouter(p)
	require.False(t, ok,
		"consul must not expose Timeouter; wait_time is a watch timeout, not a per-op timeout")
}

func TestGet_ReadsValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		wantPath string
	}{
		{
			name:     "single-segment key",
			key:      "mykey",
			value:    "myvalue",
			wantPath: "GET /v1/kv/mykey",
		},
		{
			name:     "multi-segment key preserved verbatim (no transform)",
			key:      "services/redis/host",
			value:    "cache01.internal",
			wantPath: "GET /v1/kv/services/redis/host",
		},
		{
			name:     "value returned byte-exact with no trailing-newline trim",
			key:      "raw",
			value:    "line\n",
			wantPath: "GET /v1/kv/raw",
		},
		{
			name:     "binary-safe value (embedded NUL survives base64 round-trip)",
			key:      "bin",
			value:    "a\x00b",
			wantPath: "GET /v1/kv/bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
				writeConsulValue(w, tt.key, tt.value)
			})

			p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

			got, err := p.Get(t.Context(), tt.key)
			require.NoError(t, err)

			assert.Equal(t, tt.value, got)
			assert.Equal(t, []string{tt.wantPath}, stub.requests())
		})
	}
}

func TestGet_MissingKeyReturnsKeyNotFound(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	_, err := p.Get(t.Context(), "services/absent")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound,
		"a missing key must map to *kv.KeyNotFoundError for negative_ttl_not_found caching")
	require.Equal(t, "services/absent", notFound.KeyPath)
}

func TestGet_BackendErrorReturnsStoreUnavailable(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	_, err := p.Get(t.Context(), "services/redis")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable,
		"a backend failure must map to *kv.StoreUnavailableError for negative_ttl_transient caching")
}

func TestGet_UsesQueryContextForBasicAuth(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeConsulValue(w, "k", "v")
	})

	var cfg consul.Config
	cfg.Address = addrOf(stub.url)
	cfg.HttpAuth.Username = "user"
	cfg.HttpAuth.Password = "pass"

	p := newConsulProvider(t, &cfg)

	got, err := p.Get(t.Context(), "k")
	require.NoError(t, err)
	require.Equal(t, "v", got)

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	require.Equal(t, want, stub.lastAuth())
}

func TestGet_PropagatesContextCancellation(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeConsulValue(w, "k", "v")
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := p.Get(ctx, "k")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGet_HonorsContextDeadline(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		// Sleep past the caller's deadline so the request is aborted in flight.
		// The provider must attach ctx via QueryOptions.WithContext for the
		// SecretStore's per-op deadline to actually bound the call.
		time.Sleep(200 * time.Millisecond)
		writeConsulValue(w, "k", "v")
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err := p.Get(ctx, "k")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBackwardCompatParity_ConsulGet(t *testing.T) {
	t.Run("key exists -> value returned", func(t *testing.T) {
		stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
			writeConsulValue(w, "tyk-apis/my_service_url", "https://upstream.internal")
		})

		p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

		got, err := p.Get(t.Context(), "tyk-apis/my_service_url")
		require.NoError(t, err)
		require.Equal(t, "https://upstream.internal", got)
	})

	t.Run("key absent -> not found", func(t *testing.T) {
		stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})

		p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

		_, err := p.Get(t.Context(), "tyk-apis/missing")

		var notFound *kv.KeyNotFoundError
		require.ErrorAs(t, err, &notFound)
	})
}

func TestList_ReturnsPairsUnderPrefix(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeConsulPairs(w, []struct{ Key, Value string }{
			{Key: "tyk-apis/", Value: ""}, // directory marker — must be skipped
			{Key: "tyk-apis/c2_value", Value: "http://up/"},
			{Key: "tyk-apis/auth_header", Value: "X-From-Consul"},
		})
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	got, err := lister(t, p).List(t.Context(), "tyk-apis")
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		"tyk-apis/c2_value":    "http://up/",
		"tyk-apis/auth_header": "X-From-Consul",
	}, got, "returns full keys, directory marker skipped")

	assert.Contains(t, stub.requests(), "GET /v1/kv/tyk-apis")
}

func TestList_EmptyPrefixErrorsWithoutRequest(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("List must not hit the backend for an empty prefix")
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	_, err := lister(t, p).List(t.Context(), "")
	require.Error(t, err, "empty prefix must be rejected to avoid a whole-store scan")
	require.Empty(t, stub.requests())
}

func TestList_EmptyResultIsNotError(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		writeConsulPairs(w, nil) // 200 with an empty array
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	got, err := lister(t, p).List(t.Context(), "tyk-apis")
	require.NoError(t, err, "a prefix that matches nothing is not an error")
	require.Empty(t, got)
}

func TestList_BackendErrorReturnsStoreUnavailable(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	_, err := lister(t, p).List(t.Context(), "tyk-apis")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable,
		"a backend failure must map to *kv.StoreUnavailableError, like Get")
}

func TestProvider_ImplementsSetter(t *testing.T) {
	p := newConsulProvider(t, &consul.Config{})

	_, ok := kv.AsSetter(p)
	require.True(t, ok, "consul must implement kv.Setter for the write-back path")
}

func TestSet_WritesValueVerbatim(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		wantPath string
	}{
		{
			name:     "single-segment key",
			key:      "mykey",
			value:    "myvalue",
			wantPath: "PUT /v1/kv/mykey",
		},
		{
			name:     "multi-segment key preserved verbatim (no transform)",
			key:      "tyk-apis/rotated_key",
			value:    "abc123",
			wantPath: "PUT /v1/kv/tyk-apis/rotated_key",
		},
		{
			name:     "value written byte-exact with no trailing-newline trim",
			key:      "raw",
			value:    "line\n",
			wantPath: "PUT /v1/kv/raw",
		},
		{
			name:     "binary-safe value (embedded NUL survives)",
			key:      "bin",
			value:    "a\x00b",
			wantPath: "PUT /v1/kv/bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
				// Consul answers a successful PUT with 200 and the literal "true".
				_, err := w.Write([]byte("true"))
				require.NoError(t, err)
			})

			p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

			err := setter(t, p).Set(t.Context(), tt.key, tt.value)
			require.NoError(t, err)

			assert.Equal(t, []string{tt.wantPath}, stub.requests())
			assert.Equal(t, tt.value, stub.lastBody(),
				"value must be written to consul verbatim, no transform")
		})
	}
}

func TestSet_BackendErrorReturnsStoreUnavailable(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	err := setter(t, p).Set(t.Context(), "tyk-apis/key", "v")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable,
		"a backend write failure must map to *kv.StoreUnavailableError")
	require.Equal(t, "tyk-apis/key", unavailable.KeyPath)
}

func TestSet_PropagatesContextCancellation(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("true"))
		require.NoError(t, err)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := setter(t, p).Set(ctx, "k", "v")
	require.Error(t, err,
		"a cancelled context must abort the write (WriteOptions.WithContext)")
	require.ErrorIs(t, err, context.Canceled)
}

func TestSet_UsesBasicAuth(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("true"))
		require.NoError(t, err)
	})

	var cfg consul.Config
	cfg.Address = addrOf(stub.url)
	cfg.HttpAuth.Username = "user"
	cfg.HttpAuth.Password = "pass"

	p := newConsulProvider(t, &cfg)

	require.NoError(t, setter(t, p).Set(t.Context(), "k", "v"))

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	require.Equal(t, want, stub.lastAuth())
}

func TestBackwardCompatParity_ConsulPut(t *testing.T) {
	stub := newConsulStub(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("true"))
		require.NoError(t, err)
	})

	p := newConsulProvider(t, &consul.Config{Address: addrOf(stub.url)})

	require.NoError(t, setter(t, p).Set(t.Context(), "tyk-apis/edge_api_key", "rotated-secret"))

	require.Equal(t, []string{"PUT /v1/kv/tyk-apis/edge_api_key"}, stub.requests())
	require.Equal(t, "rotated-secret", stub.lastBody())
}
