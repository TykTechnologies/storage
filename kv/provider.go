package kv

import (
	"context"
	"encoding/json"
)

// KeyValueRetriever defines the core read capability for retrieving values by key.
type KeyValueRetriever interface {
	Get(ctx context.Context, key string) (string, error)
}

// Provider is the composite interface that all KV providers must implement.
// Currently only requires read access via KeyValueRetriever, but designed
// for future expansion.
//
// Providers may optionally implement Initializer, Closer, HealthChecker,
// or Lister interfaces for additional capabilities that will be detected
// via type assertion during registry operations.
type Provider interface {
	KeyValueRetriever
}

// ProviderFactory creates a specific provider instance from raw JSON configuration.
// Each provider type registers its own factory function that knows how to parse
// its specific configuration format and return a configured Provider.
//
// The factory pattern allows the registry to create providers dynamically
// without compile-time dependencies on specific provider implementations.
type ProviderFactory func(config json.RawMessage) (Provider, error)

// Initializer is an optional interface for providers that require network
// initialization or connection establishment before use.
type Initializer interface {
	Init(ctx context.Context) error
}

// Lister is an optional interface for providers that support enumerating
// keys by prefix. This enables dynamic discovery of available secrets
// and operational tooling.
type Lister interface {
	List(ctx context.Context, prefix string) ([]string, error)
}

// Closer is an optional interface for providers that need graceful shutdown
// or resource cleanup when the registry is closed.
type Closer interface {
	Close(ctx context.Context) error
}

// AsLister attempts to extract a Lister from a Provider,
// automatically unwrapping decorators.
func AsLister(p Provider) (Lister, bool) {
	return As[Lister](p)
}

// AsInitializer attempts to extract an Initializer from a Provider,
// automatically unwrapping decorators.
func AsInitializer(p Provider) (Initializer, bool) {
	return As[Initializer](p)
}

// AsCloser attempts to extract an Closer from a Provider,
// automatically unwrapping decorators.
func AsCloser(p Provider) (Closer, bool) {
	return As[Closer](p)
}

// As attempts to extract an interface of type T from a Provider,
// automatically unwrapping decorators up to a maximum depth.
func As[T any](p Provider) (T, bool) {
	const maxDepth = 100
	var zero T

	for range maxDepth {
		if p == nil {
			return zero, false
		}

		if v, ok := p.(T); ok {
			return v, true
		}

		wrapper, ok := p.(interface{ Unwrap() Provider })
		if !ok {
			return zero, false
		}

		p = wrapper.Unwrap()
	}

	return zero, false
}
