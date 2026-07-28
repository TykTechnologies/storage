// Package consul provides a kv.Provider backed by HashiCorp Consul's KV store.
// It performs direct per-key reads (GET /v1/kv/<key>) and returns each key's
// value verbatim, leaving "#field" extraction to the resolver.
//
// Consul is a remote provider: it is not Standalone, so the registry wraps it in
// the caching / singleflight SecretStore. The client is built without any
// network I/O; the connection is established lazily on the first Get.
package consul

import (
	"context"
	"errors"
	"strings"

	"github.com/TykTechnologies/storage/kv"
	consulsdk "github.com/hashicorp/consul/api"
)

// consulProvider is a kv.Provider backed by a configured consul KV client.
type consulProvider struct {
	// kvClient is consul's KV endpoint, with the resolved Config already baked
	// into the underlying client.
	kvClient *consulsdk.KV
}

// Get reads the value at key and returns it verbatim: no trimming, no key
// transformation, no interpretation ("#field" extraction is the resolver's job).
//
// A missing key returns *kv.KeyNotFoundError and a transport/backend failure
// returns *kv.StoreUnavailableError — the two error types the negative cache
// distinguishes. ctx bounds the request via QueryOptions, so the SecretStore's
// per-operation deadline is honored.
func (cp *consulProvider) Get(ctx context.Context, key string) (string, error) {
	pair, _, err := cp.kvClient.Get(key, (&consulsdk.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if pair == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	return string(pair.Value), nil
}

// Set writes value verbatim as the raw bytes at key (PUT /v1/kv/<key>), with no
// key transformation or interpretation.
// A transport or backend failure returns *kv.StoreUnavailableError.
func (cp *consulProvider) Set(ctx context.Context, key, value string) error {
	pair := &consulsdk.KVPair{
		Key:   key,
		Value: []byte(value),
	}

	ctx, cancel := context.WithTimeout(ctx, kv.DefaultOperationTimeout)
	defer cancel()

	_, err := cp.kvClient.Put(pair, (&consulsdk.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	return nil
}

// List returns every key/value pair under prefix, keyed by the FULL consul key
// (the caller strips the prefix if it wants relative keys). Consul directory
// markers — keys ending in "/" — are skipped; they are not real entries.
//
// An empty prefix is rejected: consul would treat it as "list the entire KV
// store", which is never what a reference resolver wants and is an easy footgun.
// A prefix that matches nothing is not an error — it returns an empty map.
func (cp *consulProvider) List(ctx context.Context, prefix string) (map[string]string, error) {
	if prefix == "" {
		return nil, errors.New("consul: list requires a non-empty prefix")
	}

	ctx, cancel := context.WithTimeout(ctx, kv.DefaultOperationTimeout)
	defer cancel()

	pairs, _, err := cp.kvClient.List(prefix, (&consulsdk.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return nil, &kv.StoreUnavailableError{KeyPath: prefix, Err: err}
	}

	out := make(map[string]string, len(pairs))

	for _, p := range pairs {
		if strings.HasSuffix(p.Key, "/") {
			continue
		}

		out[p.Key] = string(p.Value)
	}

	return out, nil
}
