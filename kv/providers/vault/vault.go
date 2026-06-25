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

// TODO: Refine the doc comments for the Config type

// Config is used to configure the creation of a client
// This is a stripped down version of the config structure in vault's API client
type Config struct {
	// Address is the address of the Vault server. This should be a complete
	// URL such as "http://vault.example.com".
	Address string `json:"address"`

	// AgentAddress is the address of the local Vault agent. This should be a
	// complete URL such as "http://vault.example.com".
	AgentAddress string `json:"agent_address"`

	// MaxRetries controls the maximum number of times to retry when a vault
	// serer occurs
	MaxRetries int `json:"max_retries"`

	Timeout string `json:"timeout"`

	// Token is the vault root token
	Token string `json:"token" structviewer:"obfuscate"`

	// KVVersion is the version number of Vault. Defaults to 2
	KVVersion int `json:"kv_version"`
}

func NewFactory() kv.ProviderFactory {
	return func(rawJSON json.RawMessage) (kv.Provider, error) {
		// We want to cover case when just empty "{}" string is provided
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

		return &vaultProvider{
			client:  client,
			timeout: timeout,
			kvv2:    kvv2,
		}, nil
	}
}

// TODO: Add easy to understand comments on the provider fields
type vaultProvider struct {
	// The config is already attached to the client.
	client *api.Client
	// Timeout will be used additionally at secret store
	// to override default context timeout.
	timeout time.Duration
	// kvv2 determines the version engine to use while retrieving
	// values.
	kvv2 bool
}

// FIX:
// 1. I don't understand in what case provider can fulfill the "storeName" field
// to the KeyNotFoundError
// 2. Address warning with /data/ in path. Is it possible?
// 3.

func (vp *vaultProvider) Get(ctx context.Context, key string) (string, error) {
	logical := vp.client.Logical()

	// WARN: Is it possible that key contains the /data/ already on the key?
	// I should clarify this and check if its present before injecting /data.
	path := key

	if vp.kvv2 {
		splitted := strings.Split(key, "/")
		splitted[0] += "/data"
		path = strings.Join(splitted, "/")
	}

	secret, err := logical.ReadWithContext(ctx, path)
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if secret == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	data := secret.Data

	if vp.kvv2 {
		var ok bool
		// kvv2 wraps the contents of the secret within inner "data" field.
		data, ok = data["data"].(map[string]any)
		if !ok {
			// FIX: Do I have to return different error here? I don't think so
			return "", &kv.KeyNotFoundError{KeyPath: key}
		}
	}

	var sb strings.Builder

	err = json.NewEncoder(&sb).Encode(data)
	if err != nil {
		// FIX: Do I have to return different error here? Not sure if
		// caller should distinguish the error when the data couldn't be marshaled.
		// Probably it should return just general error.
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	return sb.String(), nil
}

func (vp *vaultProvider) Timeout() time.Duration {
	return vp.timeout
}
