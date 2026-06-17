package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
)

// initOptions collects the inputs to NewFromConfig that aren't the raw
// config document.
type initOptions struct {
	factories     map[kv.ProviderType]kv.ProviderFactory
	defaultStores map[string]kv.StoreConfig
}

// InitOption configures NewFromConfig.
type InitOption func(*initOptions)

// WithFactories injects additional provider factories (enterprise or custom)
// on top of the OSS defaults. A nil map is a no-op — the OSS build path.
func WithFactories(f map[kv.ProviderType]kv.ProviderFactory) InitOption {
	return func(o *initOptions) {
		o.factories = f
	}
}

// WithDefaultStores supplies store definitions merged with LOWER precedence
// than the kv.stores block in rawConfig — a store defined in both places
// uses the rawConfig definition.
func WithDefaultStores(s map[string]kv.StoreConfig) InitOption {
	return func(o *initOptions) {
		o.defaultStores = s
	}
}

// NewFromConfig initializes a Registry from the kv section of rawConfig.
// Internally it runs a two-phase process so that store credentials (e.g. a
// Vault token) can themselves live in an env or inline store and be
// referenced from the config.
//
// Referenceability rules:
//
//   - A store config may reference any local-type store by name —
//     wherever it is defined (WithDefaultStores or kv.stores) — because all
//     local-type stores initialize before store configs are resolved.
//   - A store config may never reference a remote store (vault, consul,
//     aws, ...): remote stores initialize concurrently, so this is an
//     unresolvable bootstrap cycle. Such references reach the provider
//     factory as literals and fail there, subject to the store's
//     required flag.
//   - Values inside an inline store's data map are always literal; a
//     kv:// reference there is never resolved.
//
// NewFromConfig does not resolve the rest of the document — the single
// strict ResolveAll pass over the full config is owned by the caller.
// A nil or empty rawConfig is valid: the kv-section parse is skipped and
// the registry is built from WithDefaultStores alone.
//
// The caller owns the returned registry, including Close on shutdown.
func NewFromConfig(
	ctx context.Context,
	rawConfig []byte,
	opts ...InitOption,
) (*Registry, error) {
	// A store's own config can contain kv:// references — e.g. a Vault store
	// whose token lives in an env var: {"token": "kv://env/VAULT_TOKEN"}.
	// Resolving that requires the env store to already exist, which forces a
	// two-phase init:
	//
	//   Phase 1 — stand up the local-type stores (env, inline, file: literal
	//     config, no network) in a throwaway registry, and use them to resolve
	//     references inside the OTHER store configs.
	//   Phase 2 — build the real registry and open every store, now that all
	//     configs are resolved. Local stores re-init cheaply here; remote
	//     stores open their connections for the first time.
	options := &initOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var config struct {
		KV kv.Config `json:"kv"`
	}

	if len(rawConfig) > 0 {
		if err := json.Unmarshal(rawConfig, &config); err != nil {
			return nil, fmt.Errorf("kv: failed to parse config: %w", err)
		}
	}

	merged := make(map[string]kv.StoreConfig, len(options.defaultStores)+len(config.KV.Stores))
	maps.Copy(merged, options.defaultStores)
	maps.Copy(merged, config.KV.Stores)

	// Phase 1: resolve references in store configs against local stores.
	if err := resolveStoreConfigReferences(ctx, merged, options.factories); err != nil {
		return nil, err
	}

	// Phase 2: build the registry and initialize all stores.
	full, err := newRegistryWithFactories(options.factories)
	if err != nil {
		return nil, err
	}

	if len(merged) > 0 {
		err := full.InitStores(ctx, &kv.Config{Stores: merged, Cache: config.KV.Cache})
		if err != nil {
			return nil, fmt.Errorf("kv: failed to initialize stores: %w", err)
		}
	}

	return full, nil
}

// resolveStoreConfigReferences is Phase 1. It initializes the local-type stores
// from merged into a TEMPORARY registry, then resolves kv:// references found
// inside the remaining (remote) store configs against them, rewriting merged in
// place. The temporary registry is discarded; Phase 2 rebuilds everything.
//
// Resolution is lenient: a reference to a store absent from the temporary
// registry — i.e. a remote store, which only exists after Phase 2 — is left
// untouched instead of failing. It reaches the provider factory as a literal in
// Phase 2 and fails there if the store was required. This is why a remote store
// config cannot reference another remote store.
func resolveStoreConfigReferences(
	ctx context.Context,
	merged map[string]kv.StoreConfig,
	factories map[kv.ProviderType]kv.ProviderFactory,
) error {
	locals := make(map[string]kv.StoreConfig)
	var hasRemotes bool

	for name, storeCfg := range merged {
		if storeCfg.Type.IsLocal() {
			locals[name] = storeCfg
		} else {
			hasRemotes = true
		}
	}

	// Only remote configs can carry references worth resolving; local configs
	// are literal. If there are none, skip the bootstrap entirely.
	if !hasRemotes {
		return nil
	}

	bootstrap, err := newRegistryWithFactories(factories)
	if err != nil {
		return err
	}

	defer func() {
		_ = bootstrap.Close(context.WithoutCancel(ctx))
	}()

	if len(locals) > 0 {
		if err := bootstrap.InitStores(ctx, &kv.Config{Stores: locals}); err != nil {
			return fmt.Errorf("kv: failed to initialize local stores for bootstrap: %w", err)
		}
	}

	lenient := resolve.NewResolver(bootstrap, resolve.WithLenientMode())

	for name, storeCfg := range merged {
		// Local store configs are literal and are initialized from the raw parse in
		// Phase 1 (env/inline/file alike), so there is nothing to resolve here.
		// Skip them, and skip stores with no config blob (ResolveAll(nil) errors).
		if storeCfg.Type.IsLocal() || len(storeCfg.Config) == 0 {
			continue
		}

		resolved, err := lenient.ResolveAll(ctx, storeCfg.Config)
		if err != nil {
			return fmt.Errorf("kv: failed to resolve config of store %q: %w", name, err)
		}

		storeCfg.Config = resolved
		merged[name] = storeCfg
	}

	return nil
}

func newRegistryWithFactories(factories map[kv.ProviderType]kv.ProviderFactory) (*Registry, error) {
	r := NewDefaultRegistry()

	for providerType, factory := range factories {
		if err := r.Add(providerType, factory); err != nil {
			return nil, fmt.Errorf("kv: failed to register factory: %w", err)
		}
	}

	return r, nil
}
