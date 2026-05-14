package kv

import (
	"encoding/json"
)

// KVConfig represents the top-level "kv" configuration block in component configs.
// It contains global settings and named store definitions.
//
// Example JSON structure:
//
//	{
//	  "kv": {
//	    "cache": {"enabled": true, "ttl": "60s"},
//	    "stores": {
//	      "vault-prod": {"type": "vault", "required": true, "config": {...}}
//	    }
//	  }
//	}
type KVConfig struct {
	Stores map[string]StoreConfig `json:"stores"`
	Cache  CacheConfig            `json:"cache"`
}

// StoreConfig defines the configuration for a single named KV store instance.
type StoreConfig struct {
	// Type specifies which provider factory to use.
	Type string `json:"type"`

	// Required determines startup behavior if the store fails to initialize.
	Required bool `json:"required"`

	// Config contains provider-specific configuration as raw JSON.
	// Each provider's factory knows how to parse its own config format.
	Config json.RawMessage `json:"config"`
}

// CacheConfig controls the caching behavior for resolved secrets.
type CacheConfig struct {
	// Enabled controls whether resolved secrets are cached in memory
	Enabled bool `json:"enabled"`

	// TTL specifies how long cached values remain valid before refresh.
	// Format: Go duration string (e.g., "60s", "5m", "1h")
	TTL string `json:"ttl"`

	RefreshBeforeExpiry  string `json:"refresh_before_expiry"`
	NegativeTTLNotFound  string `json:"negative_ttl_not_found"`
	NegativeTTLTransient string `json:"negative_ttl_transient"`
}
