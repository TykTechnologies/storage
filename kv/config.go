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
//	      "vault-prod": {"type": "vault", "required": true, "config": {...}}
//	    }
//	  }
//	}
type Config struct {
	Stores map[string]StoreConfig `json:"stores"`
}

// FIX: Validate me
// StoreConfig defines the configuration for a single named KV store instance.
type StoreConfig struct {
	// Type specifies which provider factory to use.
	Type ProviderType `json:"type"`

	// Required determines startup behavior if the store fails to initialize.
	Required bool `json:"required"`

	// Config contains provider-specific configuration as raw JSON.
	// Each provider's factory knows how to parse its own config format.
	Config json.RawMessage `json:"config"`
}
