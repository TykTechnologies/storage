// Package vault provides a kv.Provider backed by HashiCorp Vault. It reads
// secrets from Vault's KV engine (v1 or v2) and returns each secret's data map
// serialized as JSON, leaving field selection to the resolver's "#field" syntax.
//
// Unlike the local providers (env, file, inline), vault is remote: it is not
// Standalone and exposes a Timeouter, so the registry wraps it in the caching /
// singleflight SecretStore. The Vault client is created without any network I/O;
// the connection is established lazily on the first Get.
package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/hashicorp/vault/api"
)

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
	return func(rawJSON json.RawMessage) (kv.Provider, error) {
		// Empty/absent config: json.Unmarshal would fail cryptically on it, so
		// reject it with a clear message. A present-but-empty object ("{}") is
		// left to the token check below.
		if len(rawJSON) == 0 {
			return nil, errors.New("vault: config is missing")
		}

		var conf Config

		if err := json.Unmarshal(rawJSON, &conf); err != nil {
			return nil, fmt.Errorf("vault: invalid config: %w", err)
		}

		if conf.Token == "" {
			return nil, errors.New("vault: token is required")
		}

		defaultCfg := api.DefaultConfig()

		if conf.Address != "" {
			defaultCfg.Address = conf.Address
		}

		if conf.AgentAddress != "" {
			defaultCfg.AgentAddress = conf.AgentAddress
		}

		if conf.MaxRetries > 0 {
			defaultCfg.MaxRetries = conf.MaxRetries
		}

		var timeout time.Duration

		if conf.Timeout != "" {
			d, err := time.ParseDuration(conf.Timeout)
			if err != nil {
				return nil, fmt.Errorf(
					"vault: invalid timeout %q: %w",
					conf.Timeout,
					err,
				)
			}

			timeout = d
		}

		if timeout > 0 {
			defaultCfg.Timeout = timeout
		}

		client, err := api.NewClient(defaultCfg)
		if err != nil {
			return nil, fmt.Errorf("vault: failed to create a client: %w", err)
		}

		client.SetToken(conf.Token)

		kvv2 := conf.KVVersion != 1

		// trim trailing slash(es) so "tenants/a/kv/" and "tenants/a/kv" behave the same
		mountPath := strings.TrimRight(conf.MountPath, "/")

		return &vaultProvider{
			client:    client,
			timeout:   timeout,
			kvv2:      kvv2,
			mountPath: mountPath,
		}, nil
	}
}

// vaultProvider is a kv.Provider backed by a configured Vault API client.
type vaultProvider struct {
	// client is the Vault API client. The resolved Config (address, token,
	// retries, timeout) is already baked into it at construction.
	client *api.Client

	// timeout is the parsed Config.Timeout, surfaced via Timeout() so the
	// SecretStore wrapper can bound each Get with it. 0 means "unset", letting
	// the store use its own default.
	timeout time.Duration

	// kvv2 selects the read path: when true, Get injects "/data" after the
	// mount and unwraps the v2 "data" envelope; when false it reads the path
	// as-is (KV v1).
	kvv2 bool

	// mountPath is the normalized (trailing slash trimmed) Config.MountPath. When
	// non-empty it overrides the legacy first-segment "/data" injection for KV v2
	// and confines keys to that mount.
	mountPath string
}

// Get reads the secret at key and returns its data map serialized as JSON.
// Field selection (the "#field" fragment) is the resolver's responsibility, so
// Get returns the whole secret, never a single value. When mount_path is
// configured, key must be the full logical path under that mount.
//
// A missing secret returns *kv.KeyNotFoundError; a backend or transport failure
// returns *kv.StoreUnavailableError.
func (vp *vaultProvider) Get(ctx context.Context, key string) (string, error) {
	path, err := vp.physicalPath(key)
	if err != nil {
		return "", err
	}

	secret, err := vp.client.Logical().ReadWithContext(ctx, path)
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if secret == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	data := secret.Data

	if vp.kvv2 {
		var ok bool
		// KV v2 wraps the secret in an inner "data" field. Its absence means the
		// path holds no v2 secret, which we treat as not-found.
		data, ok = data["data"].(map[string]any)
		if !ok {
			return "", &kv.KeyNotFoundError{KeyPath: key}
		}
	}

	b, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("vault: failed to encode secret %q: %w", key, err)
	}

	return string(b), nil
}

func (vp *vaultProvider) Timeout() time.Duration {
	return vp.timeout
}

// physicalPath maps the caller's logical key to the physical path the Vault HTTP
// API expects. For KV v1 the logical and physical paths are identical. For KV v2
// the engine stores secrets under "<mount>/data/<path>", so the "/data/" segment
// is inserted after the mount:
//
//   - With mount_path set, immediately after that (possibly multi-segment) mount.
//     key must be the full logical path under the mount; otherwise it is rejected.
//     The check is on the path-segment boundary (mountPath + "/"), so a mount of
//     "secret" does not match an unrelated "secrets/..." key.
//   - With mount_path empty, after the first path segment — the legacy assumption
//     that the mount is a single top-level segment. The injection is intentionally
//     blind: a segment legitimately named "data" must not be special-cased.
func (vp *vaultProvider) physicalPath(key string) (string, error) {
	if !vp.kvv2 {
		return key, nil
	}

	if vp.mountPath != "" {
		if !strings.HasPrefix(key, vp.mountPath+"/") {
			return "", fmt.Errorf("vault: key %q is not under mount_path %q", key, vp.mountPath)
		}

		return vp.mountPath + "/data" + strings.TrimPrefix(key, vp.mountPath), nil
	}

	splitted := strings.Split(key, "/")
	splitted[0] += "/data"

	return strings.Join(splitted, "/"), nil
}
