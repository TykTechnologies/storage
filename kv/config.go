package kv

import (
	"encoding/json"
)

// Config represents the top-level "kv" configuration block in component configs.
// It contains global settings and named store definitions.
//
// Example JSON structure:
//
//	{
//	  "kv": {
//	    "stores": {
//	      "vault-prod": {"type": "hashicorp_vault", "required": true, "config": {...}}
//	    }
//	  }
//	}
type Config struct {
	// Stores lists the secret backends Tyk is allowed to read from, each under a
	// name you choose. That name is how a secret reference picks the store: a
	// store named "vault-prod" is reached as kv://vault-prod/<key>, or with the
	// inline $kv{vault-prod/<key>} form. Names are yours to invent — keep them
	// short and stable, because renaming one breaks every reference that uses
	// it. Configure as many stores as you need, including several of the same
	// type (one Vault store per environment, for instance).
	Stores map[string]StoreConfig `json:"stores"`
}

// StoreConfig defines the configuration for a single named KV store instance.
type StoreConfig struct {
	// Type is the kind of secret backend this store talks to. It also decides
	// how the Config block below is interpreted, since every backend takes its
	// own settings. One of:
	//   - "env"                 environment variables of the Tyk process
	//   - "inline"              literal values written into this configuration
	//   - "file"                files on the local filesystem
	//   - "hashicorp_vault"     HashiCorp Vault
	//   - "hashicorp_consul"    the key/value store of HashiCorp Consul
	//   - "aws_secrets_manager" AWS Secrets Manager
	//   - "gcp_secret_manager"  Google Cloud Secret Manager
	//   - "azure_key_vault"     Azure Key Vault
	// Required.
	Type ProviderType `json:"type"`

	// Required says whether Tyk may start up without this store. A store fails
	// to start when its settings cannot be read, its credentials are refused, or
	// its type is not supported.
	//
	// Left false — the default — such a failure is written to the log as a
	// warning and the store is skipped: Tyk starts, and any reference to that
	// store fails at the point something tries to read it. Set to true, the same
	// failure stops Tyk from starting and reports the error. Choose true for
	// stores holding secrets Tyk cannot run properly without, so a mistake shows
	// up immediately at startup rather than later as a failing API.
	Required bool `json:"required"`

	// Config holds the settings for the backend named by Type: where it lives,
	// how Tyk authenticates to it, and any behaviour options. The fields differ
	// per backend and are documented with the Config type in each provider's own
	// package.
	Config json.RawMessage `json:"config"`
}
