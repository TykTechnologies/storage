package kv

import "context"

// SecretStore is the high-level interface. It wraps Provider
// implementations with additional capabilities like caching,
// single-flight deduplication, and enhanced error handling.
type SecretStore interface {
	// GetSecret retrieves a secret value with caching and deduplication.
	GetSecret(ctx context.Context, path string) (string, error)
}
