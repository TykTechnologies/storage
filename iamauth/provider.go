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
	"strings"
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
	// RefreshBeforeExpiry is how far ahead of expiry tokens are refreshed,
	// expressed as a Go duration string (for example "5m"). This mirrors the
	// consumer's raw config value so callers don't each re-implement parsing.
	// Empty applies the provider-specific default. Optional.
	RefreshBeforeExpiry string
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
		refresh, err := parseRefreshBeforeExpiry(cfg.RefreshBeforeExpiry)
		if err != nil {
			return nil, err
		}

		return gcp.NewCredentialsProvider(ctx, gcp.Config{
			ServiceAccount:      cfg.ServiceAccount,
			RefreshBeforeExpiry: refresh,
		})
	default:
		return nil, fmt.Errorf("iamauth: unsupported provider %q", cfg.Provider)
	}
}

// parseRefreshBeforeExpiry parses the optional refresh-before-expiry duration
// string. An empty value yields a zero duration, letting the provider apply its
// own default. Centralizing it here means Gateway, Pump, MDCB and Dashboard all
// pass their raw config string straight through instead of each duplicating
// this parse-and-default logic.
func parseRefreshBeforeExpiry(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("iamauth: invalid refresh_before_expiry %q: %w", raw, err)
	}

	return d, nil
}
