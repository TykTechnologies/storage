package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/hashicorp/consul/api"
)

type Config struct {
	// Address is the address of the Consul server
	Address string `json:"address"`

	// Scheme is the URI scheme for the Consul server
	Scheme string `json:"scheme"`

	// The datacenter to use. If not provided, the default agent datacenter is used.
	Datacenter string `json:"datacenter"`

	// HttpAuth is the auth info to use for http access.
	HttpAuth struct {
		// Username to use for HTTP Basic Authentication
		Username string `json:"username"`

		// Password to use for HTTP Basic Authentication
		Password string `json:"password" structviewer:"obfuscate"`
	} `json:"http_auth"`

	// WaitTime limits how long a Watch will block. If not provided,
	// the agent default values will be used.
	WaitTime string `json:"wait_time"`

	// Token is used to provide a per-request ACL token
	// which overrides the agent's default token.
	Token string `json:"token" structviewer:"obfuscate"`

	// TLS configuration
	TLSConfig struct {
		// Address
		Address string `json:"address"`
		// CA file
		CAFile string `json:"ca_file"`
		// CA Path
		CAPath string `json:"ca_path"`
		// Cert file
		CertFile string `json:"cert_file"`
		// Key file
		KeyFile string `json:"key_file"`
		// Disable TLS validation
		InsecureSkipVerify bool `json:"insecure_skip_verify"`
	} `json:"tls_config"`
}

func NewFactory() kv.ProviderFactory {
	return func(rawJSON json.RawMessage) (kv.Provider, error) {
		var conf Config

		if len(rawJSON) != 0 {
			if err := json.Unmarshal(rawJSON, &conf); err != nil {
				return nil, fmt.Errorf("consul: invalid config: %w", err)
			}
		}

		defaultCfg := api.DefaultConfig()

		if conf.Address != "" {
			defaultCfg.Address = conf.Address
		}

		if conf.Scheme != "" {
			defaultCfg.Scheme = conf.Scheme
		}

		if conf.Datacenter != "" {
			defaultCfg.Datacenter = conf.Datacenter
		}

		if conf.HttpAuth.Username != "" || conf.HttpAuth.Password != "" {
			defaultCfg.HttpAuth = &api.HttpBasicAuth{
				Username: conf.HttpAuth.Username,
				Password: conf.HttpAuth.Password,
			}
		}

		var waitTime time.Duration

		if conf.WaitTime != "" {
			d, err := time.ParseDuration(conf.WaitTime)
			if err != nil {
				return nil, fmt.Errorf(
					"consul: invalid wait_time %q: %w",
					conf.WaitTime,
					err,
				)
			}

			waitTime = d
		}

		if waitTime > 0 {
			defaultCfg.WaitTime = waitTime
		}

		if conf.Token != "" {
			defaultCfg.Token = conf.Token
		}

		if conf.TLSConfig.Address != "" {
			defaultCfg.TLSConfig.Address = conf.TLSConfig.Address
		}

		if conf.TLSConfig.CertFile != "" {
			defaultCfg.TLSConfig.CertFile = conf.TLSConfig.CertFile
		}

		if conf.TLSConfig.CAFile != "" {
			defaultCfg.TLSConfig.CAFile = conf.TLSConfig.CAFile
		}

		if conf.TLSConfig.InsecureSkipVerify {
			defaultCfg.TLSConfig.InsecureSkipVerify = conf.TLSConfig.InsecureSkipVerify
		}

		client, err := api.NewClient(defaultCfg)
		if err != nil {
			return nil, fmt.Errorf("consul: failed to create client: %w", err)
		}

		return &consulProvider{
			kvClient: client,
		}, nil
	}
}

type consulProvider struct {
	kvClient *api.Client
}

func (cp *consulProvider) Get(ctx context.Context, key string) (string, error) {
	var queryOptions *api.QueryOptions

	pair, _, err := cp.kvClient.KV().Get(key, queryOptions.WithContext(ctx))
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if pair == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	return string(pair.Value), nil
}
