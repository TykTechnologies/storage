package kv

import (
	"context"
	"encoding/json"
	"time"
)

// ProviderType represents the unique string identifier for a KV provider.
type ProviderType string

const (
	// --- Open Source (OSS) Providers ---

	// Env resolves secrets from environment variables.
	Env ProviderType = "env"

	// Inline resolves secrets from plain text in the configuration.
	Inline ProviderType = "inline"

	// Vault resolves secrets from HashiCorp Vault.
	Vault ProviderType = "hashicorp_vault"

	// Consul resolves secrets from HashiCorp Consul.
	Consul ProviderType = "hashicorp_consul"

	// K8s resolves secrets from Kubernetes Secrets mounted as files.
	K8s ProviderType = "k8s_files"

	// --- Enterprise Edition (EE) Providers ---

	// AWS resolves secrets from AWS Secrets Manager.
	AWS ProviderType = "aws_secrets_manager"

	// GCP resolves secrets from Google Cloud Secret Manager.
	GCP ProviderType = "gcp_secret_manager"

	// Azure resolves secrets from Azure Key Vault.
	Azure ProviderType = "azure_key_vault"

	// Conjur resolves secrets from CyberArk Conjur.
	Conjur ProviderType = "cyberark_conjur"
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

// Standalone is an optional interface for providers that do not need
// to be combined with caching or singleflight mechanisms.
type Standalone interface {
	IsStandalone() bool
}

// Timeouter is an optional interface for providers that expose a custom
// duration configuration for operations.
type Timeouter interface {
	Timeout() time.Duration
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

// AsStandalone attempts to extract a Standalone from a Provider.
func AsStandalone(p Provider) (Standalone, bool) {
	return As[Standalone](p)
}

// AsTimeouter attempts to extract a Timeouter from a Provider.
func AsTimeouter(p Provider) (Timeouter, bool) {
	return As[Timeouter](p)
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
