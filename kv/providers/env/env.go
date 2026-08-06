// Package env provides a KV provider that reads secrets from the process
// environment. It is a Standalone provider (no caching): environment lookups
// are in-process and cheap, and a rotated value is visible on the next read.
//
// A lookup name is built as Prefix + key, with the key optionally uppercased
// (see Config).
//
// Security: the Prefix is a confinement boundary. Every key is read as
// Prefix+key, so a reference can only reach variables the operator deliberately
// placed under that prefix — never arbitrary process variables (cloud
// credentials, tokens, PATH, …). This matters when references come from a
// less-trusted source than the host. An empty prefix removes that boundary,
// so it is rejected by default: every Get returns ErrPrefixRequired.
// Setting AllowNoPrefix opts back into reading process env directly,
// and is only safe when every reference source is as trusted as the host.
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

// ErrPrefixRequired is returned by Get when the provider has an empty prefix and
// AllowNoPrefix is not set.
var ErrPrefixRequired = errors.New("env: prefix is required")

// Config is used to configure an environment-variable store.
//
// One behaviour to be aware of when using this store: an environment variable
// that does not exist and one set to an empty string cannot be told apart, so a
// misspelled key quietly resolves to an empty value instead of being reported as
// missing. Where a wrong value must not pass unnoticed, prefer a store that
// distinguishes the two.
type Config struct {
	// Prefix goes in front of every key to form the name of the environment
	// variable Tyk reads. With a prefix of "TYK_SECRET_", the reference
	// kv://<store-name>/db_password reads the variable TYK_SECRET_db_password
	// (see Uppercase for matching upper-case variable names). The prefix is used
	// exactly as written.
	//
	// It is required, and it is what confines references to the variables you
	// meant to expose. Since every lookup is prefix + key, a reference can only
	// reach variables you deliberately named with that prefix, and never the rest
	// of the environment Tyk runs with — cloud credentials, tokens, PATH and so
	// on. That matters because references are written in API definitions, which
	// may come from people less trusted than whoever runs the Tyk host.
	//
	// With no prefix set, every read fails instead of falling back to reading the
	// whole environment. Use AllowNoPrefix if that fallback is genuinely what you
	// want.
	Prefix string `json:"prefix"`

	// Uppercase converts the key to upper case before the lookup, so that
	// kv://<store-name>/db_password with a prefix of "TYK_SECRET_" reads
	// TYK_SECRET_DB_PASSWORD. Use it when your environment variables follow the
	// usual upper-case convention but references are written in lower case. The
	// prefix itself is never converted — write it in the case you need. Defaults
	// to false, taking keys exactly as given. Optional.
	Uppercase bool `json:"uppercase"`

	// AllowNoPrefix permits a store with an empty prefix, which reads environment
	// variables directly by name with none of the confinement described under
	// Prefix. Every variable in Tyk's environment then becomes readable through a
	// reference, so turn this on only where every reference is as trusted as the
	// host itself. Defaults to false, so a prefix left out by mistake fails
	// safely rather than quietly opening up the environment.
	AllowNoPrefix bool `json:"allow_no_prefix"`
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
			prefix:        cfg.Prefix,
			uppercase:     cfg.Uppercase,
			allowNoPrefix: cfg.AllowNoPrefix,
		}, nil
	}
}

type envProvider struct {
	prefix        string
	uppercase     bool
	allowNoPrefix bool
}

// Get reads the environment variable named Prefix + (uppercased key if
// Uppercase) and returns its value.
//
// If the prefix is empty and AllowNoPrefix is not set it returns
// ErrPrefixRequired for every key — the prefix guard is checked first, so even
// an empty key is rejected before any lookup. Otherwise the result mirrors
// os.Getenv exactly: a missing variable and a variable set to "" are
// indistinguishable, and both return ("", nil); an empty key reads
// os.Getenv(Prefix) and likewise returns no error.
func (ep *envProvider) Get(_ context.Context, key string) (string, error) {
	if ep.prefix == "" && !ep.allowNoPrefix {
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
