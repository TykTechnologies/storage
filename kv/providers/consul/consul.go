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
