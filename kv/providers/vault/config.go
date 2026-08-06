package vault

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/kv"
	vaultsdk "github.com/hashicorp/vault/api"
)

// FIX: Validate me
// Config is the JSON "config" block of a vault store.
type Config struct {
	// Address is the Vault server URL, e.g. "https://vault.example.com:8200".
	// Optional: when empty the Vault client default is used (the VAULT_ADDR
	// environment variable, otherwise https://127.0.0.1:8200).
	Address string `json:"address"`

	// AgentAddress is the URL of a local Vault Agent, e.g. "http://127.0.0.1:8100".
	// When set, the client routes requests through the agent instead of Address.
	// A token is still required.
	AgentAddress string `json:"agent_address"`

	// MaxRetries caps how many times the client retries a request after a
	// server (5xx) error. Applied only when > 0; otherwise the Vault client
	// default is kept.
	MaxRetries int `json:"max_retries"`

	// Timeout bounds each Vault request. It is a Go duration string such as
	// "5s" or "500ms"; an empty value means "unset", leaving the SecretStore to
	// apply its own default.
	Timeout string `json:"timeout"`

	// Token authenticates requests to Vault. Required.
	Token string `json:"token"`

	// KVVersion selects the KV secrets engine version. Any value other than 1
	// means v2 (the default): secrets live under "<mount>/data/<path>" and are
	// wrapped in a "data" envelope. 1 selects v1, where the path is used as-is.
	KVVersion int `json:"kv_version"`

	// MountPath is the path the KV secrets engine is mounted at, e.g. "secret"
	// or a nested "tenants/a/kv". It is OPTIONAL and only affects KV v2.
	//
	// The key passed to Get is always the full logical path under this mount
	// (it must start with MountPath). When set, the provider inserts the v2
	// "/data/" segment immediately after MountPath instead of assuming the mount
	// is the first path segment — which is what makes nested mounts work. A key
	// that is not under MountPath is rejected.
	//
	// When empty, the provider falls back to the legacy behavior of injecting
	// "/data" after the first segment, so existing single-segment-mount configs
	// are unaffected. Ignored for KV v1 (which has no data segment).
	MountPath string `json:"mount_path"`
}

// NewFactory returns a kv.ProviderFactory for HashiCorp Vault stores.
//
// The factory parses the provider Config and constructs a Vault API client, but
// performs no network I/O: the connection to Vault is established lazily on the
// first Get. It returns an error only for config that is present but unusable:
//   - malformed JSON,
//   - an unparseable timeout (must be a Go duration string, e.g. "5s"),
//   - a missing token. Vault has no usable zero value, so a token is required
//     even when agent_address is set.
//
// The resulting provider is remote: it is NOT Standalone and exposes its timeout
// via the Timeouter interface, so the registry wraps it in the caching /
// singleflight SecretStore and bounds each Get with the configured timeout.
func NewFactory() kv.ProviderFactory {
	return func(raw json.RawMessage) (kv.Provider, error) {
		var conf Config
		if err := parseConfig(raw, &conf); err != nil {
			return nil, err
		}

		if err := conf.validate(); err != nil {
			return nil, err
		}

		timeout, err := conf.parsedTimeout()
		if err != nil {
			return nil, err
		}

		client, err := conf.newClient(timeout)
		if err != nil {
			return nil, err
		}

		client.SetToken(conf.Token)

		return &vaultProvider{
			client:  client,
			timeout: timeout,
			kvv2:    conf.KVVersion != 1,
			// trim trailing slash(es) so "tenants/a/kv/" and "tenants/a/kv" behave the same
			mountPath: strings.TrimRight(conf.MountPath, "/"),
		}, nil
	}
}

func parseConfig(raw json.RawMessage, conf *Config) error {
	if len(raw) == 0 {
		return errors.New("vault: config is missing")
	}

	if err := json.Unmarshal(raw, conf); err != nil {
		return fmt.Errorf("vault: invalid config: %w", err)
	}

	return nil
}

func (conf *Config) validate() error {
	if conf.Token == "" {
		return errors.New("vault: token is required")
	}

	return nil
}

func (conf *Config) parsedTimeout() (time.Duration, error) {
	if conf.Timeout == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(conf.Timeout)
	if err != nil {
		return 0, fmt.Errorf("vault: invalid timeout %q: %w", conf.Timeout, err)
	}

	return d, nil
}

func (conf *Config) newClient(timeout time.Duration) (*vaultsdk.Client, error) {
	defaultCfg := vaultsdk.DefaultConfig()

	if conf.Address != "" {
		defaultCfg.Address = conf.Address
	}

	if conf.AgentAddress != "" {
		defaultCfg.AgentAddress = conf.AgentAddress
	}

	if conf.MaxRetries > 0 {
		defaultCfg.MaxRetries = conf.MaxRetries
	}

	if timeout > 0 {
		defaultCfg.Timeout = timeout
	}

	client, err := vaultsdk.NewClient(defaultCfg)
	if err != nil {
		return nil, fmt.Errorf("vault: create client: %w", err)
	}

	return client, nil
}
