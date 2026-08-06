package vault

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/TykTechnologies/storage/kv"
	vaultsdk "github.com/hashicorp/vault/api"
)

// Config is used to configure a HashiCorp Vault store.
type Config struct {
	// Address is the URL of the Vault server Tyk reads secrets from, for example
	// "https://vault.example.com:8200". Leave it empty and the address is taken
	// from the VAULT_ADDR environment variable of the Tyk component this store is
	// configured in, falling back to https://127.0.0.1:8200 — a Vault on the same
	// host as that component. Optional, but set it explicitly unless you are
	// deliberately relying on VAULT_ADDR.
	Address string `json:"address"`

	// AgentAddress is the URL of a Vault Agent running alongside Tyk, for example
	// "http://127.0.0.1:8100". Vault Agent is a HashiCorp companion process that
	// sits between an application and Vault, handling login and token renewal on
	// its behalf. Set this and Tyk sends its requests to the agent instead of to
	// Address. A token is still required either way. Left empty, the
	// VAULT_AGENT_ADDR environment variable applies if the component's
	// environment has it set; with neither, Tyk talks to Vault directly.
	// Optional.
	AgentAddress string `json:"agent_address"`

	// MaxRetries is how many additional attempts Tyk makes when Vault answers a
	// request with a server-side error (an HTTP 5xx, typically a Vault node that
	// is sealed, standby, or briefly overloaded). Retries are a Vault client
	// feature and use exponential backoff. Leave it at 0 to keep the Vault
	// client's own default of 2 retries, or whatever VAULT_MAX_RETRIES says where
	// the component's environment has it set; a value above 0 replaces both.
	// Optional.
	MaxRetries int `json:"max_retries"`

	// Timeout is how long Tyk waits for a single Vault request — reading or
	// writing one secret — before giving up and reporting the store as
	// unavailable. Give it as a Go duration string: "5s", "500ms", "1m".
	// Defaults to 5s when omitted; a value Tyk cannot read as a duration stops
	// the store from starting. Optional.
	Timeout string `json:"timeout"`

	// Token is the Vault token Tyk authenticates with. Every Vault request
	// carries it, and the policies attached to it decide which secrets Tyk can
	// read. Required — including when AgentAddress is set, as Vault has no
	// usable "no token" mode.
	//
	// Note that Vault tokens expire. For a long-running Tyk deployment, prefer a
	// token whose lifetime you manage outside Tyk — via Vault Agent, or by
	// renewing and re-issuing the configuration — over a short-lived token that
	// will silently stop working.
	Token string `json:"token"`

	// KVVersion is the version of Vault's KV secrets engine that holds the
	// secrets. Vault has two, and they store data differently, so Tyk has to
	// know which it is talking to. Set it to 1 for KV version 1; any other
	// value, including leaving it unset, means version 2 — the current default
	// in Vault and the one you get unless you chose otherwise when enabling the
	// engine. If reads fail with secrets you know exist, this is the first field
	// to check. Optional.
	KVVersion int `json:"kv_version"`

	// MountPath is where the KV secrets engine is mounted in Vault — "secret"
	// for a stock setup, or something nested such as "tenants/a/kv". Tyk needs
	// this because KV version 2 stores secrets one level below the mount
	// internally, and Tyk has to insert that step in the right place when
	// building the request.
	//
	// Keys in references are always the full path including the mount, for
	// example kv://vault-prod/secret/my-app/db with a mount of "secret". A key
	// that does not sit under MountPath is rejected.
	//
	// Optional, and only used for KV version 2. Leave it empty and Tyk assumes
	// the mount is the first segment of the key, which is correct for the usual
	// single-segment mounts; set it whenever your mount has more than one
	// segment.
	MountPath string `json:"mount_path"`

	// Namespace confines every request to a Vault namespace, for example "team-a"
	// or a nested "team-a/prod". Namespaces are isolated tenants within one Vault
	// cluster, each with its own mounts, policies and secrets, so the same key
	// can exist in several of them.
	//
	// Namespaces are a feature of Vault Enterprise and HCP Vault only. Against
	// Community Vault, setting this makes every request fail. Leading and
	// trailing slashes and spaces are tidied up, so "team-a/prod/" and
	// "team-a/prod" are equivalent.
	//
	// Optional. Leaving it empty does not mean the root namespace: Tyk then
	// inherits whatever the VAULT_NAMESPACE environment variable of its process
	// says, the same way Address falls back to VAULT_ADDR. All stores that omit
	// this field therefore share that one namespace, so set it per store when they
	// need to differ.
	//
	// The name is not checked when the store starts — Tyk makes no request to
	// Vault at that point — so a namespace that does not exist, is spelled wrongly,
	// or is unavailable on your Vault licence only shows up as reads failing.
	Namespace string `json:"namespace"`
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

		// Trim any leading/trailing mix of slashes and whitespace in one pass so
		// copy-pasted values like "team-a/prod/", " team-a/prod", and "/ team-a"
		// all normalize to "team-a/prod" (interior slashes are preserved). Empty
		// means "unset": we skip SetNamespace so an inherited VAULT_NAMESPACE is
		// left intact and the client sends no header.
		if ns := strings.TrimFunc(conf.Namespace, func(r rune) bool {
			return r == '/' || unicode.IsSpace(r)
		}); ns != "" {
			client.SetNamespace(ns)
		}

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
