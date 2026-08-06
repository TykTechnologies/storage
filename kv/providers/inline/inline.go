// Package inline provides a KV provider that serves secrets from a literal
// key/value map embedded in the configuration. It is a Standalone provider (no
// caching): lookups are an in-memory map read, with nothing to refresh.
package inline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/TykTechnologies/storage/kv"
)

// ErrEmptyKey is returned by Get for an empty key.
var ErrEmptyKey = errors.New("inline: key must not be empty")

// Config is used to configure an inline store.
type Config struct {
	// Data holds the values themselves, as key/value pairs written straight into
	// this configuration: with {"db_password": "s3cret"}, the reference
	// kv://<store-name>/db_password resolves to "s3cret".
	//
	// Because the values sit in the configuration in plain text, wherever that
	// configuration is stored and whoever can read it, this store suits
	// development, testing and values that are not really secret. Anything that
	// genuinely needs protecting belongs in one of the other backends.
	//
	// Keys are matched exactly: nothing is added to them and case matters.
	// Leaving it empty is valid — every read is then reported as not found.
	Data map[string]string `json:"data"`
}

// NewFactory returns a ProviderFactory for inline stores.
//
// An empty or absent config is valid and yields a provider with no data: the
// store registers unconditionally and every Get is not-found until data is
// supplied. Invalid JSON returns an error.
func NewFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		if len(config) == 0 {
			return &inlineProvider{}, nil
		}

		var cfg Config
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("inline: invalid config: %w", err)
		}

		return &inlineProvider{data: cfg.Data}, nil
	}
}

// inlineProvider serves secrets from a map built once at construction and never
// mutated, so concurrent Get reads are safe without locking.
type inlineProvider struct {
	data map[string]string
}

// Get returns the value stored under key.
//
// An empty key returns ErrEmptyKey. A key that is absent from the map returns
// *kv.KeyNotFoundError. The lookup is exact: no prefix, no case folding.
// A key present with value "" is distinct from a missing key and returns ("", nil).
func (ip *inlineProvider) Get(_ context.Context, key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	v, ok := ip.data[key]
	if !ok {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	return v, nil
}

// IsStandalone reports that the provider needs no cache wrapper: inline data is
// in-memory and literal, and there is nothing to refresh.
func (ip *inlineProvider) IsStandalone() bool {
	return true
}
