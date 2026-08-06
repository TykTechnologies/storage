package consul

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/kv"
	consulsdk "github.com/hashicorp/consul/api"
)

// FIX: Validate me
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
	return func(raw json.RawMessage) (kv.Provider, error) {
		var conf Config
		if err := parseConfig(raw, &conf); err != nil {
			return nil, err
		}

		clientCfg, err := conf.clientConfig()
		if err != nil {
			return nil, err
		}

		client, err := consulsdk.NewClient(clientCfg)
		if err != nil {
			return nil, fmt.Errorf("consul: create client: %w", err)
		}

		return &consulProvider{kvClient: client.KV()}, nil
	}
}

func parseConfig(raw json.RawMessage, conf *Config) error {
	if len(raw) == 0 {
		return nil
	}

	if err := json.Unmarshal(raw, conf); err != nil {
		return fmt.Errorf("consul: invalid config: %w", err)
	}

	return nil
}

func (conf *Config) clientConfig() (*consulsdk.Config, error) {
	clientCfg := consulsdk.DefaultConfig()

	if conf.Address != "" {
		clientCfg.Address = conf.Address
	}

	if conf.Scheme != "" {
		clientCfg.Scheme = conf.Scheme
	}

	if conf.Datacenter != "" {
		clientCfg.Datacenter = conf.Datacenter
	}

	if conf.Token != "" {
		clientCfg.Token = conf.Token
	}

	if conf.HttpAuth.Username != "" || conf.HttpAuth.Password != "" {
		clientCfg.HttpAuth = &consulsdk.HttpBasicAuth{
			Username: conf.HttpAuth.Username,
			Password: conf.HttpAuth.Password,
		}
	}

	if err := conf.applyWaitTime(clientCfg); err != nil {
		return nil, err
	}

	conf.applyTLSConfig(clientCfg)

	return clientCfg, nil
}

func (conf *Config) applyWaitTime(clientCfg *consulsdk.Config) error {
	if conf.WaitTime == "" {
		return nil
	}

	waitTime, err := time.ParseDuration(conf.WaitTime)
	if err != nil {
		return fmt.Errorf("consul: invalid wait_time %q: %w", conf.WaitTime, err)
	}

	if waitTime > 0 {
		clientCfg.WaitTime = waitTime
	}

	return nil
}

func (conf *Config) applyTLSConfig(clientCfg *consulsdk.Config) {
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
