// Package consul provides a kv.Provider backed by HashiCorp Consul's KV store.
// It performs direct per-key reads (GET /v1/kv/<key>) and returns each key's
// value verbatim, leaving "#field" extraction to the resolver.
//
// Consul is a remote provider: it is not Standalone, so the registry wraps it in
// the caching / singleflight SecretStore. The client is built without any
// network I/O; the connection is established lazily on the first Get.
package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/hashicorp/consul/api"
)

// Config is the JSON "config" block of a consul store.
type Config struct {
	// Address of the consul agent as host:port; the scheme comes from Scheme.
	Address string `json:"address"`

	// Scheme is the URI scheme ("http" or "https"). Defaults to "http".
	Scheme string `json:"scheme"`

	// Datacenter to query. Empty uses the agent's default datacenter.
	Datacenter string `json:"datacenter"`

	// HttpAuth holds optional HTTP Basic Auth credentials.
	HttpAuth struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"http_auth"`

	// WaitTime is consul's blocking-query (watch) timeout, as a Go duration
	// string such as "5s". It is NOT a per-operation timeout — our plain reads
	// never block on it — which is why the provider does not implement Timeouter.
	// Empty or "0s" leaves the agent default.
	WaitTime string `json:"wait_time"`

	// Token is used to provide a per-request ACL token
	// which overrides the agent's default token.
	Token string `json:"token"`

	// TLSConfig configures TLS for https connections.
	TLSConfig struct {
		Address            string `json:"address"`
		CAFile             string `json:"ca_file"`
		CAPath             string `json:"ca_path"`
		CertFile           string `json:"cert_file"`
		KeyFile            string `json:"key_file"`
		InsecureSkipVerify bool   `json:"insecure_skip_verify"`
	} `json:"tls_config"`
}

// NewFactory returns a kv.ProviderFactory for HashiCorp Consul stores.
//
// The factory parses Config and builds a consul API client, but performs no
// network I/O. It errors only for config that is present but unusable: malformed
// JSON, an unparseable wait_time, or TLS settings the client rejects (e.g. an
// unreadable ca_file). Absent config is valid — an empty blob builds a client
// against consul's default local agent.
func NewFactory() kv.ProviderFactory {
	return func(rawJSON json.RawMessage) (kv.Provider, error) {
		var conf Config

		// Absent config is a valid state: skip the unmarshal so nil/empty input
		// falls through to consul's defaults instead of failing to parse.
		if len(rawJSON) != 0 {
			if err := json.Unmarshal(rawJSON, &conf); err != nil {
				return nil, fmt.Errorf("consul: invalid config: %w", err)
			}
		}

		clientCfg := api.DefaultConfig()

		if conf.Address != "" {
			clientCfg.Address = conf.Address
		}

		if conf.Scheme != "" {
			clientCfg.Scheme = conf.Scheme
		}

		if conf.Datacenter != "" {
			clientCfg.Datacenter = conf.Datacenter
		}

		if conf.HttpAuth.Username != "" || conf.HttpAuth.Password != "" {
			clientCfg.HttpAuth = &api.HttpBasicAuth{
				Username: conf.HttpAuth.Username,
				Password: conf.HttpAuth.Password,
			}
		}

		if conf.WaitTime != "" {
			waitTime, err := time.ParseDuration(conf.WaitTime)
			if err != nil {
				return nil, fmt.Errorf("consul: invalid wait_time %q: %w", conf.WaitTime, err)
			}

			if waitTime > 0 {
				clientCfg.WaitTime = waitTime
			}
		}

		if conf.Token != "" {
			clientCfg.Token = conf.Token
		}

		applyTLSConfig(clientCfg, &conf)

		client, err := api.NewClient(clientCfg)
		if err != nil {
			return nil, fmt.Errorf("consul: create client: %w", err)
		}

		return &consulProvider{kvClient: client.KV()}, nil
	}
}

func applyTLSConfig(clientCfg *api.Config, conf *Config) {
	tls := conf.TLSConfig

	if tls.Address != "" {
		clientCfg.TLSConfig.Address = tls.Address
	}

	if tls.CAFile != "" {
		clientCfg.TLSConfig.CAFile = tls.CAFile
	}

	if tls.CAPath != "" {
		clientCfg.TLSConfig.CAPath = tls.CAPath
	}

	if tls.CertFile != "" {
		clientCfg.TLSConfig.CertFile = tls.CertFile
	}

	if tls.KeyFile != "" {
		clientCfg.TLSConfig.KeyFile = tls.KeyFile
	}

	if tls.InsecureSkipVerify {
		clientCfg.TLSConfig.InsecureSkipVerify = true
	}
}

// consulProvider is a kv.Provider backed by a configured consul KV client.
type consulProvider struct {
	// kvClient is consul's KV endpoint, with the resolved Config already baked
	// into the underlying client.
	kvClient *api.KV
}

// Get reads the value at key and returns it verbatim: no trimming, no key
// transformation, no interpretation ("#field" extraction is the resolver's job).
//
// A missing key returns *kv.KeyNotFoundError and a transport/backend failure
// returns *kv.StoreUnavailableError — the two error types the negative cache
// distinguishes. ctx bounds the request via QueryOptions, so the SecretStore's
// per-operation deadline is honored.
func (cp *consulProvider) Get(ctx context.Context, key string) (string, error) {
	pair, _, err := cp.kvClient.Get(key, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if pair == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	return string(pair.Value), nil
}
