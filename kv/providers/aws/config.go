package aws

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/TykTechnologies/storage/kv"
)

// Config is the JSON "config" block of an aws_secrets_manager store.
type Config struct {
	// Region is the AWS region the store reads from. Required — each named
	// store is explicit about where it points, mirroring GCP's project_id.
	Region string `json:"region"`

	// Endpoint overrides the Secrets Manager endpoint URL. Useful for API
	// emulators (moto, LocalStack) and VPC interface endpoints. Optional.
	Endpoint string `json:"endpoint"`

	// AccessKeyID / SecretAccessKey are static credentials. Both must be set
	// together. When absent, the SDK default credential chain applies
	// (env vars, shared config, IMDS, IRSA). Optional.
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`

	// SessionToken accompanies temporary static credentials. Only meaningful
	// with AccessKeyID/SecretAccessKey. Optional.
	SessionToken string `json:"session_token"`

	// Profile selects a shared-config profile as the credential source.
	// Mutually exclusive with static keys. Optional.
	Profile string `json:"profile"`

	// RoleARN, when set, makes the provider assume this IAM role via STS on
	// top of the base credentials (static keys, profile, or default chain).
	// Optional.
	RoleARN string `json:"role_arn"`

	// ExternalID is passed to the AssumeRole call for cross-account trust
	// policies. Requires RoleARN. Optional.
	ExternalID string `json:"external_id"`

	// RoleSessionName names the assumed-role session. Requires RoleARN. Optional.
	RoleSessionName string `json:"role_session_name"`

	// VersionStage pins reads to a staging label (default AWSCURRENT).
	// Mutually exclusive with VersionID. Optional.
	VersionStage string `json:"version_stage"`

	// VersionID pins reads to an exact version. Mutually exclusive with
	// VersionStage. Optional.
	VersionID string `json:"version_id"`

	// Timeout bounds each API call. Go duration string ("5s", "500ms"). Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline, when true, strips a single trailing "\n" from the
	// payload. Guards the common footgun of secrets created with a trailing
	// newline (e.g. `echo val | aws secretsmanager create-secret ...`). Optional.
	TrimTrailingNewline bool `json:"trim_trailing_newline"`
}

// NewFactory returns the factory that validates a store's config and builds the
// provider. Validation is exhaustive and fails loud here;
// the client is built later in Init, and no network call is made.
func NewFactory() kv.ProviderFactory {
	return func(raw json.RawMessage) (kv.Provider, error) {
		var config Config
		if err := parseConfig(raw, &config); err != nil {
			return nil, err
		}

		if err := config.validate(); err != nil {
			return nil, err
		}

		timeout, err := config.parsedTimeout()
		if err != nil {
			return nil, err
		}

		return &awsProvider{
			cfg:     &config,
			timeout: timeout,
		}, nil
	}
}

func parseConfig(raw json.RawMessage, config *Config) error {
	if len(raw) == 0 {
		return errors.New("aws: config is missing")
	}

	if err := json.Unmarshal(raw, config); err != nil {
		return fmt.Errorf("aws: invalid config: %w", err)
	}

	return nil
}

func (cfg *Config) validate() error {
	if cfg.Region == "" {
		return errors.New("aws: region is required")
	}

	if err := cfg.validateCredentials(); err != nil {
		return err
	}

	if cfg.VersionStage != "" && cfg.VersionID != "" {
		return errors.New("aws: version_stage and version_id are mutually exclusive")
	}

	return cfg.validateEndpoint()
}

func (cfg *Config) validateCredentials() error {
	hasKey := cfg.AccessKeyID != ""
	hasSecret := cfg.SecretAccessKey != ""

	if hasKey != hasSecret {
		return errors.New("aws: access_key_id and secret_access_key must be set together")
	}

	if cfg.SessionToken != "" && !hasKey {
		return errors.New("aws: session_token set without access_key_id/secret_access_key")
	}

	if cfg.Profile != "" && hasKey {
		return errors.New("aws: profile and static access keys are mutually exclusive")
	}

	if cfg.RoleARN == "" {
		if cfg.ExternalID != "" {
			return errors.New("aws: external_id set without role_arn")
		}

		if cfg.RoleSessionName != "" {
			return errors.New("aws: role_session_name set without role_arn")
		}
	}

	return nil
}

func (cfg *Config) validateEndpoint() error {
	if cfg.Endpoint == "" {
		return nil
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("aws: invalid endpoint %q: %w", cfg.Endpoint, err)
	}

	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("aws: invalid endpoint %q: want an http(s) URL", cfg.Endpoint)
	}

	return nil
}

func (cfg *Config) parsedTimeout() (time.Duration, error) {
	if cfg.Timeout == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return 0, fmt.Errorf("aws: invalid timeout %q: %w", cfg.Timeout, err)
	}

	if d < 0 {
		return 0, nil
	}

	return d, nil
}
