// Package env provides a KV provider that reads secrets from the process
// environment. It is a Standalone provider (no caching): environment lookups
// are in-process and cheap, and a rotated value is visible on the next read.
//
// A lookup name is built as Prefix + key, with the key optionally uppercased
// (see Config).
//
// Security: a non-empty Prefix acts as a confinement boundary — every key is
// read as Prefix+key, so a reference can only ever reach variables under that
// prefix (there is no traversal escape for environment names). An empty Prefix
// removes that boundary: keys are read verbatim, so the store can read any
// variable in the process environment.
package env

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/TykTechnologies/storage/kv"
)

// FIX: I've asked Andy and Pete, waiting for the decision about next two:
// 1. The prefix has the same meaning as base_path for the file provider. It restricts
// attacker to access any env variables. It looks like we should force the prefix right?
// Because otherwise its too permisive as well. I should ask Andy about it.
// 2. If we have a reference like kv://env/KEY#field the resolver will try to find
// the field within KEY JSON value and if its missing or not json, the error will
// occurred. Is it desired or we should handle a specific case for env variable search
// that is never failing just returning an empty value?

// Config is the env provider's configuration.
type Config struct {
	// Prefix is prepended literally to the key before the environment lookup.
	// It is never uppercased itself. Empty is valid and reads the key with no prefix.
	Prefix string `json:"prefix"`

	// Uppercase, when true, uppercases the key — not the prefix — before lookup,
	// so Get("my_key") with prefix "TYK_SECRET_" reads "TYK_SECRET_MY_KEY".
	Uppercase bool `json:"uppercase"`
}

// NewFactory returns a ProviderFactory for environment-backed stores.
//
// An empty or absent config is valid and yields a zero-Config provider: no
// prefix, no uppercasing, keys read verbatim from the environment. Invalid JSON
// returns an error; that malformed config is the provider's only error path.
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
// The result mirrors os.Getenv exactly: a missing variable and a variable set
// to "" are indistinguishable, and both return ("", nil). An empty key reads
// os.Getenv(Prefix) and likewise returns no error.
//
// Deliberate contrast with the file provider: env never returns
// *kv.KeyNotFoundError. A not-found error would propagate as a fatal startup
// failure in the caller's env resolution path, regressing any config that
// tolerates an unset optional secret.
func (ep *envProvider) Get(_ context.Context, key string) (string, error) {
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
