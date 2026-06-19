// Package env provides a KV provider that reads secrets from the process
// environment. It is a Standalone provider (no caching): environment lookups
// are in-process and cheap, and a rotated value is visible on the next read.
//
// A lookup name is built as Prefix + key, with the key optionally uppercased
// (see Config).
//
// Security: the Prefix is a mandatory confinement boundary — the env analogue of
// the file provider's base_path. Every key is read as Prefix+key, so a reference
// can only ever reach variables under that prefix (there is no traversal escape
// for environment names). A store with no prefix would let any reference read any
// process variable (cloud credentials, tokens, PATH, …), so an unprefixed store
// is disabled: every Get returns ErrPrefixRequired and resolves nothing.
package env

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/TykTechnologies/storage/kv"
)

// ErrPrefixRequired is returned by Get when the provider has no prefix
// configured.
var ErrPrefixRequired = errors.New("env: prefix is required")

// FIX: still open — if a reference like kv://env/KEY#field reads a value that is
// missing or not JSON, the resolver's field extraction errors. Decide whether
// that is desired or env lookups with a fragment should fail soft to "".

// Config is the env provider's configuration.
type Config struct {
	// Prefix is prepended literally to the (optionally uppercased) key before the
	// environment lookup; it is never uppercased itself. It is mandatory: it is
	// the store's security boundary, so a provider with an empty prefix rejects
	// every Get with ErrPrefixRequired (see the package doc).
	Prefix string `json:"prefix"`

	// Uppercase, when true, uppercases the key — not the prefix — before lookup,
	// so Get("my_key") with prefix "TYK_SECRET_" reads "TYK_SECRET_MY_KEY".
	Uppercase bool `json:"uppercase"`
}

// NewFactory returns a ProviderFactory for environment-backed stores.
//
// An empty or absent config still builds a provider, so the store can be
// registered unconditionally.
func NewFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		if len(config) == 0 {
			return &envProvider{}, nil
		}

		var cfg Config

		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("env: invalid config: %w", err)
		}

		return &envProvider{
			prefix:    cfg.Prefix,
			uppercase: cfg.Uppercase,
		}, nil
	}
}

type envProvider struct {
	prefix    string
	uppercase bool
}

// Get reads the environment variable named Prefix + (uppercased key if
// Uppercase) and returns its value.
//
// If no prefix is configured it returns ErrPrefixRequired for every key — the
// prefix guard is checked first, so even an empty key is rejected before any
// lookup. With a prefix set, the result mirrors os.Getenv exactly: a missing
// variable and a variable set to "" are indistinguishable, and both return
// ("", nil); an empty key reads os.Getenv(Prefix) and likewise returns no error.
func (ep *envProvider) Get(_ context.Context, key string) (string, error) {
	if ep.prefix == "" {
		return "", ErrPrefixRequired
	}

	if ep.uppercase {
		key = strings.ToUpper(key)
	}

	return os.Getenv(ep.prefix + key), nil
}

// IsStandalone reports that the provider needs no cache wrapper: environment
// reads are in-process and cheap, and there is nothing to refresh.
func (ep *envProvider) IsStandalone() bool {
	return true
}
