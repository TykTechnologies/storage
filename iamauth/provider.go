// Package iamauth builds cloud-provider credential providers for authenticating
// to managed Redis/Valkey (for example GCP Memorystore) with short-lived IAM
// access tokens instead of a static password.
//
// It is the single place the provider-specific cloud SDKs live. Components
// (Gateway, Pump, MDCB, Dashboard) call NewProvider with their configured
// provider and pass the result to the storage connector's
// model.WithCredentialsProvider option. A new cloud (for example AWS
// ElastiCache) is added by implementing another sub-package and one extra case
// in NewProvider, without touching any consumer.
package iamauth

import (
	"context"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/iamauth/gcp"
	"github.com/TykTechnologies/storage/temporal/model"
)

// Supported provider identifiers for Config.Provider.
const (
	ProviderGCP = "gcp"
)

// Config selects and configures an IAM credentials provider.
type Config struct {
	// Provider is the cloud provider identifier, for example "gcp".
	Provider string
	// ServiceAccount, when set, is the service account to impersonate. When
	// empty the workload's own identity is used (GKE Workload Identity or
	// GOOGLE_APPLICATION_CREDENTIALS for GCP). Optional.
	ServiceAccount string
	// RefreshBeforeExpiry is how far ahead of expiry tokens are refreshed.
	// Provider-specific default applies when zero. Optional.
	RefreshBeforeExpiry time.Duration
}

// NewProvider builds the credentials provider for cfg.Provider, suitable for the
// storage connector's model.WithCredentialsProvider option. It returns an error
// when the provider is unset or unsupported, or when the underlying provider
// fails to initialize (for example unresolved credentials).
func NewProvider(ctx context.Context, cfg Config) (model.CredentialsProviderFunc, error) {
	switch cfg.Provider {
	case "":
		return nil, fmt.Errorf("iamauth: provider must be set")
	case ProviderGCP:
		return gcp.NewCredentialsProvider(ctx, gcp.Config{
			ServiceAccount:      cfg.ServiceAccount,
			RefreshBeforeExpiry: cfg.RefreshBeforeExpiry,
		})
	default:
		return nil, fmt.Errorf("iamauth: unsupported provider %q", cfg.Provider)
	}
}
