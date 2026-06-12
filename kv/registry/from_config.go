package registry

import (
	"context"
	"encoding/json"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
)

type initOption struct {
	factories     map[kv.ProviderType]kv.ProviderFactory
	defaultStores map[string]kv.StoreConfig
}

type InitOption func(*initOption)

// WithFactories injects additional provider factories (enterprise or custom)
// on top of the OSS defaults. A nil map is a no-op — the OSS build path.
func WithFactories(f map[kv.ProviderType]kv.ProviderFactory) InitOption {
	return func(o *initOption) {
		if f != nil {
			o.factories = f
		}
	}
}

// WithDefaultStores supplies store definitions merged with LOWER precedence
// than the kv.stores block in rawConfig — a store defined in both places
// uses the rawConfig definition.
func WithDefaultStores(s map[string]kv.StoreConfig) InitOption {
	return func(o *initOption) {
		o.defaultStores = s
	}
}

func NewFromConfig(
	ctx context.Context,
	rawConfig []byte,
	opts ...InitOption,
) (*Registry, error) {
	initRegistry := NewDefaultRegistry()

	option := &initOption{
		factories:     make(map[kv.ProviderType]kv.ProviderFactory),
		defaultStores: make(map[string]kv.StoreConfig),
	}
	for _, opt := range opts {
		opt(option)
	}

	for pType, pFactory := range option.factories {
		err := initRegistry.Add(pType, pFactory)
		if err != nil {
			return nil, err
		}
	}

	var config struct {
		KV kv.Config `json:"kv"`
	}

	// FIX this more elegantly
	if rawConfig == nil {
		rawConfig = []byte(`{"kv": {}}`)
	}

	if err := json.Unmarshal(rawConfig, &config); err != nil {
		// IT looks like we have to log warning here but not return an error right?
		return nil, err
	}

	// FIX: What if default stores contain references to kv://
	// I'm doing smashing first and then marshaling and resolving.
	// Not sure if its a good way to achieve solid result there.j
	inlineOrEnv := make(map[string]kv.StoreConfig)
	merged := make(map[string]kv.StoreConfig)
	for name, storeCfg := range config.KV.Stores {
		if storeCfg.Type == kv.Env || storeCfg.Type == kv.Inline {
			inlineOrEnv[name] = storeCfg

			continue
		}

		merged[name] = storeCfg
	}

	for name, storeCfg := range option.defaultStores {
		if storeCfg.Type == kv.Env || storeCfg.Type == kv.Inline {
			inlineOrEnv[name] = storeCfg

			continue
		}

		if _, ok := merged[name]; ok {
			continue
		}

		merged[name] = storeCfg
	}

	config.KV.Stores = inlineOrEnv

	err := initRegistry.InitStores(ctx, &config.KV)
	if err != nil {
		return nil, err
	}

	config.KV.Stores = merged

	rawConfig, err = json.Marshal(config)
	if err != nil {
		return nil, err
	}

	bootstrapRegistry := NewDefaultRegistry()
	bootstrapRegistry.stores = initRegistry.stores
	bootstrapRegistry.factories = initRegistry.factories

	resolver := resolve.NewResolver(bootstrapRegistry, resolve.WithLenientMode())
	resolvedRawCfg, err := resolver.ResolveAll(ctx, rawConfig)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(resolvedRawCfg, &config); err != nil {
		return nil, err
	}

	err = bootstrapRegistry.InitStores(ctx, &config.KV)
	if err != nil {
		return nil, err
	}

	return initRegistry, nil
}
