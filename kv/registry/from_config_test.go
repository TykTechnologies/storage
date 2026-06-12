package registry_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/registry"
	"github.com/TykTechnologies/storage/kv/resolver"
	"github.com/stretchr/testify/require"
)

// fakeProvider serves a fixed data map. A nil map means every key resolves
// to "value".
type fakeProvider struct {
	data     map[string]string
	initFunc func(ctx context.Context) error
	calls    atomic.Int32
	closed   atomic.Bool
}

func (p *fakeProvider) Get(_ context.Context, key string) (string, error) {
	p.calls.Add(1)

	if p.data == nil {
		return "value", nil
	}

	v, ok := p.data[key]
	if !ok {
		return "", &kv.KeyNotFoundError{StoreName: "fake", KeyPath: key}
	}

	return v, nil
}

func (p *fakeProvider) Init(ctx context.Context) error {
	if p.initFunc != nil {
		return p.initFunc(ctx)
	}

	return nil
}

func (p *fakeProvider) Close(_ context.Context) error {
	p.closed.Store(true)
	return nil
}

// configRecorder captures every config a factory receives.
type configRecorder struct {
	mu      sync.Mutex
	configs []json.RawMessage
}

func (r *configRecorder) record(cfg json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.configs = append(r.configs, cfg)
}

func (r *configRecorder) all() []json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]json.RawMessage(nil), r.configs...)
}

// single asserts the factory was called exactly once and returns the config.
func (r *configRecorder) single(t *testing.T) json.RawMessage {
	t.Helper()

	configs := r.all()
	require.Len(t, configs, 1)

	return configs[0]
}

func recordingFactory(rec *configRecorder, p kv.Provider, err error) kv.ProviderFactory {
	return func(cfg json.RawMessage) (kv.Provider, error) {
		if rec != nil {
			rec.record(cfg)
		}

		if err != nil {
			return nil, err
		}

		return p, nil
	}
}

func dataFactory(rec *configRecorder) kv.ProviderFactory {
	return func(cfg json.RawMessage) (kv.Provider, error) {
		if rec != nil {
			rec.record(cfg)
		}

		var parsed struct {
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(cfg, &parsed); err != nil {
			return nil, err
		}

		return &fakeProvider{data: parsed.Data}, nil
	}
}

func tokenOf(t *testing.T, cfg json.RawMessage) string {
	t.Helper()

	var parsed struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal(cfg, &parsed))

	return parsed.Token
}

func newRegistry(t *testing.T, rawConfig []byte, opts ...registry.InitOption) *registry.Registry {
	t.Helper()

	reg, err := registry.NewFromConfig(t.Context(), rawConfig, opts...)
	require.NoError(t, err)
	require.NotNil(t, reg)
	t.Cleanup(func() {
		_ = reg.Close(context.WithoutCancel(t.Context()))
	})

	return reg
}

func TestNewFromConfigInitializesStoresFromKVSection(t *testing.T) {
	t.Parallel()

	doc := []byte(`{
		"listen_port": 8080,
		"kv": {
			"stores": {
				"my-values": {"type": "inline", "config": {"data": {"greeting": "hello"}}},
				"vault":     {"type": "hashicorp_vault", "config": {"token": "literal-token"}}
			}
		}
	}`)

	reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
		kv.Inline: dataFactory(nil),
		kv.Vault:  recordingFactory(nil, &fakeProvider{}, nil),
	}))

	inline, err := reg.GetStore("my-values")
	require.NoError(t, err)

	got, err := inline.Get(t.Context(), "greeting")
	require.NoError(t, err)
	require.Equal(t, "hello", got)

	_, err = reg.GetStore("vault")
	require.NoError(t, err)

	_, err = reg.GetStore("unknown")
	require.ErrorIs(t, err, kv.ErrStoreNotFound)
}

func TestNewFromConfigPhase1Resolution(t *testing.T) {
	t.Parallel()

	t.Run("remote config referencing an env store is resolved before the factory runs", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"env":   {"type": "env", "config": {"data": {"VAULT_TOKEN": "hvs.from-env"}}},
					"vault": {"type": "hashicorp_vault", "config": {"token": "kv://env/VAULT_TOKEN"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Env:   dataFactory(nil),
			kv.Vault: recordingFactory(vaultRec, &fakeProvider{}, nil),
		}))

		require.Equal(t, "hvs.from-env", tokenOf(t, vaultRec.single(t)))
	})

	t.Run("remote config referencing a user-defined inline store is resolved", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"my-values": {"type": "inline", "config": {"data": {"vault_token": "hvs.from-inline"}}},
					"vault":     {"type": "hashicorp_vault", "config": {"token": "kv://my-values/vault_token"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Inline: dataFactory(nil),
			kv.Vault:  recordingFactory(vaultRec, &fakeProvider{}, nil),
		}))

		require.Equal(t, "hvs.from-inline", tokenOf(t, vaultRec.single(t)))
	})

	t.Run("references inside WithDefaultStores configs are resolved too", func(t *testing.T) {
		t.Parallel()

		// The promoted-legacy-store shape: the default vault store's token
		// references an env store defined in the user's kv.stores block.
		doc := []byte(`{
			"kv": {
				"stores": {
					"env": {"type": "env", "config": {"data": {"VAULT_TOKEN": "hvs.for-default"}}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		newRegistry(t, doc,
			registry.WithDefaultStores(map[string]kv.StoreConfig{
				"vault": {Type: kv.Vault, Required: true, Config: json.RawMessage(`{"token": "kv://env/VAULT_TOKEN"}`)},
			}),
			registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
				kv.Env:   dataFactory(nil),
				kv.Vault: recordingFactory(vaultRec, &fakeProvider{}, nil),
			}),
		)

		require.Equal(t, "hvs.for-default", tokenOf(t, vaultRec.single(t)))
	})

	t.Run("inline store data values are never resolved", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"env":       {"type": "env", "config": {"data": {"REAL": "resolved-value"}}},
					"my-values": {"type": "inline", "config": {"data": {"keep": "kv://env/REAL"}}}
				}
			}
		}`)

		inlineRec := &configRecorder{}
		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Env:    dataFactory(nil),
			kv.Inline: dataFactory(inlineRec),
		}))

		// The literal must survive end to end: in every config the inline
		// factory ever received, and through Get on the initialized store.
		for _, cfg := range inlineRec.all() {
			var parsed struct {
				Data map[string]string `json:"data"`
			}
			require.NoError(t, json.Unmarshal(cfg, &parsed))
			require.Equal(t, "kv://env/REAL", parsed.Data["keep"])
		}

		store, err := reg.GetStore("my-values")
		require.NoError(t, err)

		got, err := store.Get(t.Context(), "keep")
		require.NoError(t, err)
		require.Equal(t, "kv://env/REAL", got)
	})
}

func TestNewFromConfigUnresolvableReferences(t *testing.T) {
	t.Parallel()

	// errsOnUnresolved simulates a real remote provider rejecting a config
	// whose credential is still a kv:// literal.
	errsOnUnresolved := func(rec *configRecorder) kv.ProviderFactory {
		return func(cfg json.RawMessage) (kv.Provider, error) {
			rec.record(cfg)

			var parsed struct {
				Token string `json:"token"`
			}
			if err := json.Unmarshal(cfg, &parsed); err != nil {
				return nil, err
			}

			if len(parsed.Token) >= 5 && parsed.Token[:5] == "kv://" {
				return nil, errors.New("invalid token: looks like an unresolved KV reference")
			}

			return &fakeProvider{}, nil
		}
	}

	t.Run("remote-to-remote reference passes through and fails a required store", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"consul": {"type": "hashicorp_consul", "config": {"token": "literal"}},
					"vault":  {"type": "hashicorp_vault", "required": true, "config": {"token": "kv://consul/token"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		reg, err := registry.NewFromConfig(t.Context(), doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Consul: recordingFactory(nil, &fakeProvider{}, nil),
			kv.Vault:  errsOnUnresolved(vaultRec),
		}))
		require.Error(t, err)
		require.Nil(t, reg)

		// Phase 1 must have left the remote-to-remote reference verbatim.
		require.Equal(t, "kv://consul/token", tokenOf(t, vaultRec.single(t)))
	})

	t.Run("optional store with unresolvable reference is skipped, others initialize", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"consul": {"type": "hashicorp_consul", "config": {"token": "literal"}},
					"vault":  {"type": "hashicorp_vault", "required": false, "config": {"token": "kv://absent-store/token"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Consul: recordingFactory(nil, &fakeProvider{}, nil),
			kv.Vault:  errsOnUnresolved(vaultRec),
		}))

		require.Equal(t, "kv://absent-store/token", tokenOf(t, vaultRec.single(t)))

		_, err := reg.GetStore("vault")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)

		_, err = reg.GetStore("consul")
		require.NoError(t, err)
	})

	t.Run("malformed reference in a store config fails fast, remote factory never runs", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"vault": {"type": "hashicorp_vault", "config": {"token": "kv://no-path-separator"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		reg, err := registry.NewFromConfig(t.Context(), doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Vault: recordingFactory(vaultRec, &fakeProvider{}, nil),
		}))
		require.ErrorIs(t, err, resolver.ErrMalformedReference)
		require.Nil(t, reg)
		require.Empty(t, vaultRec.all(), "Phase 1 must fail before any remote factory is invoked")
	})
}

func TestNewFromConfigDefaultStoresMerge(t *testing.T) {
	t.Parallel()

	t.Run("nil rawConfig builds the registry from defaults alone", func(t *testing.T) {
		t.Parallel()

		reg := newRegistry(t, nil,
			registry.WithDefaultStores(map[string]kv.StoreConfig{
				"env": {Type: kv.Env, Required: true, Config: json.RawMessage(`{"data": {"KEY": "from-default"}}`)},
			}),
			registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
				kv.Env: dataFactory(nil),
			}),
		)

		store, err := reg.GetStore("env")
		require.NoError(t, err)

		got, err := store.Get(t.Context(), "KEY")
		require.NoError(t, err)
		require.Equal(t, "from-default", got)
	})

	t.Run("kv.stores definition wins over a same-named default store", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"vault": {"type": "hashicorp_vault", "config": {"token": "from-config"}}
				}
			}
		}`)

		vaultRec := &configRecorder{}
		newRegistry(t, doc,
			registry.WithDefaultStores(map[string]kv.StoreConfig{
				"vault": {Type: kv.Vault, Config: json.RawMessage(`{"token": "from-defaults"}`)},
			}),
			registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
				kv.Vault: recordingFactory(vaultRec, &fakeProvider{}, nil),
			}),
		)

		require.Equal(t, "from-config", tokenOf(t, vaultRec.single(t)),
			"user-defined kv.stores entry must override the promoted default")
	})
}

func TestNewFromConfigDocumentHandling(t *testing.T) {
	t.Parallel()

	t.Run("references outside the kv section are ignored", func(t *testing.T) {
		t.Parallel()

		// Both an absent-store reference AND a malformed one outside kv:
		// Phase 1 must not even look at them. The caller's strict
		// ResolveAll pass owns the rest of the document.
		doc := []byte(`{
			"secret": "kv://not-a-store/key",
			"broken": "kv://no-path-separator",
			"kv": {
				"stores": {
					"my-values": {"type": "inline", "config": {"data": {"k": "v"}}}
				}
			}
		}`)

		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Inline: dataFactory(nil),
		}))

		_, err := reg.GetStore("my-values")
		require.NoError(t, err)
	})

	t.Run("empty and kv-less documents build a defaults-only registry", func(t *testing.T) {
		t.Parallel()

		for _, doc := range [][]byte{
			nil,
			{},
			[]byte(`{}`),
			[]byte(`{"listen_port": 8080}`),
			[]byte(`{"kv": {}}`),
		} {
			reg := newRegistry(t, doc,
				registry.WithDefaultStores(map[string]kv.StoreConfig{
					"env": {Type: kv.Env, Config: json.RawMessage(`{"data": {}}`)},
				}),
				registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
					kv.Env: dataFactory(nil),
				}),
			)

			_, err := reg.GetStore("env")
			require.NoError(t, err, "doc: %q", doc)
		}
	})

	t.Run("no options and nil config still yield a usable empty registry", func(t *testing.T) {
		t.Parallel()

		// The OSS build path: no enterprise factories, no defaults, no file.
		reg, err := registry.NewFromConfig(t.Context(), nil)
		require.NoError(t, err)
		require.NotNil(t, reg)

		_, err = reg.GetStore("anything")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	t.Run("nil WithFactories map is accepted", func(t *testing.T) {
		t.Parallel()

		reg, err := registry.NewFromConfig(t.Context(), nil, registry.WithFactories(nil))
		require.NoError(t, err)
		require.NotNil(t, reg)
	})

	t.Run("invalid JSON errors", func(t *testing.T) {
		t.Parallel()

		reg, err := registry.NewFromConfig(t.Context(), []byte(`{not json`))
		require.Error(t, err)
		require.Nil(t, reg)
	})
}

func TestNewFromConfigUnknownProviderType(t *testing.T) {
	t.Parallel()

	t.Run("required store with unknown type fails initialization", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"mystery": {"type": "does_not_exist", "required": true, "config": {}}
				}
			}
		}`)

		reg, err := registry.NewFromConfig(t.Context(), doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Inline: dataFactory(nil),
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does_not_exist")
		require.Nil(t, reg)
	})

	t.Run("optional store with unknown type is skipped, others initialize", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"mystery":   {"type": "does_not_exist", "required": false, "config": {}},
					"my-values": {"type": "inline", "config": {"data": {"k": "v"}}}
				}
			}
		}`)

		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Inline: dataFactory(nil),
		}))

		_, err := reg.GetStore("mystery")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)

		_, err = reg.GetStore("my-values")
		require.NoError(t, err)
	})
}

func TestNewFromConfigCacheConfig(t *testing.T) {
	t.Parallel()

	t.Run("kv.cache settings reach the store wrapper", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"cache": {"enabled": true, "ttl": "1m"},
				"stores": {
					"remote": {"type": "hashicorp_vault", "config": {}}
				}
			}
		}`)

		provider := &fakeProvider{}
		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Vault: recordingFactory(nil, provider, nil),
		}))

		store, err := reg.GetStore("remote")
		require.NoError(t, err)

		for range 3 {
			_, err = store.Get(t.Context(), "some/key")
			require.NoError(t, err)
		}

		require.Equal(t, int32(1), provider.calls.Load(), "enabled cache must serve repeat Gets")
	})

	t.Run("without cache config every Get reaches the provider", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"remote": {"type": "hashicorp_vault", "config": {}}
				}
			}
		}`)

		provider := &fakeProvider{}
		reg := newRegistry(t, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Vault: recordingFactory(nil, provider, nil),
		}))

		store, err := reg.GetStore("remote")
		require.NoError(t, err)

		for range 3 {
			_, err = store.Get(t.Context(), "some/key")
			require.NoError(t, err)
		}

		require.Equal(t, int32(3), provider.calls.Load())
	})
}

func TestNewFromConfigLifecycle(t *testing.T) {
	t.Run("context cancellation propagates and partial stores are cleaned up", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			doc := []byte(`{
				"kv": {
					"stores": {
						"blocked": {"type": "hashicorp_vault", "required": true, "config": {}},
						"healthy": {"type": "hashicorp_consul", "required": true, "config": {}}
					}
				}
			}`)

			healthy := &fakeProvider{}
			blocked := &fakeProvider{
				initFunc: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
			}

			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()

			reg, err := registry.NewFromConfig(ctx, doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
				kv.Vault:  recordingFactory(nil, blocked, nil),
				kv.Consul: recordingFactory(nil, healthy, nil),
			}))
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, reg)

			synctest.Wait()
			require.True(t, healthy.closed.Load(), "stores initialized before the failure must be closed")
		})
	})

	t.Run("Close shuts down every initialized store", func(t *testing.T) {
		t.Parallel()

		doc := []byte(`{
			"kv": {
				"stores": {
					"a": {"type": "hashicorp_vault", "config": {}},
					"b": {"type": "hashicorp_consul", "config": {}}
				}
			}
		}`)

		providerA := &fakeProvider{}
		providerB := &fakeProvider{}

		reg, err := registry.NewFromConfig(t.Context(), doc, registry.WithFactories(map[kv.ProviderType]kv.ProviderFactory{
			kv.Vault:  recordingFactory(nil, providerA, nil),
			kv.Consul: recordingFactory(nil, providerB, nil),
		}))
		require.NoError(t, err)

		require.NoError(t, reg.Close(t.Context()))
		require.True(t, providerA.closed.Load())
		require.True(t, providerB.closed.Load())

		_, err = reg.GetStore("a")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})
}
