package aws

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/TykTechnologies/storage/kv"
)

// Config is used to configure an AWS Secrets Manager store.
type Config struct {
	// Region is the AWS region that holds the secrets, for example "eu-west-2".
	// A store reads secrets from this one region only; to read secrets from a
	// second region, configure a second store. Required.
	Region string `json:"region"`

	// Endpoint is the address Tyk sends its Secrets Manager requests to. Leave
	// it empty to use the standard AWS endpoint for the configured region. Set
	// it to send requests elsewhere instead — typically a VPC interface
	// endpoint, so the traffic stays inside your own network and never crosses
	// the public internet, or a local emulator such as LocalStack when
	// developing. Must be an http(s) URL. Optional.
	Endpoint string `json:"endpoint"`

	// AccessKeyID and SecretAccessKey are the two halves of an AWS IAM access
	// key — the values AWS shows as "Access key ID" and "Secret access key"
	// when you create an access key. They are not fields of a secret: they
	// identify the IAM user Tyk authenticates as, and that user needs
	// permission to read the secrets (the secretsmanager:GetSecretValue
	// action, plus PutSecretValue and CreateSecret if Tyk also writes them).
	//
	// Both are optional and must be set together. If omitted, the credentials
	// are obtained from the host instead, using the first of these sources
	// that provides them:
	//   - the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN
	//     environment variables;
	//   - the AWS shared configuration files on the host (see Profile below);
	//   - the IAM role attached to the EC2 instance or ECS task Tyk runs on;
	//   - the IAM role attached to Tyk's Kubernetes service account, when
	//     running on EKS with IAM Roles for Service Accounts (IRSA).
	// On a host that already carries an IAM role, leaving both empty is the
	// recommended setup: no long-lived keys are stored in Tyk's configuration.
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`

	// SessionToken is needed only when AccessKeyID and SecretAccessKey are
	// short-lived credentials issued by AWS STS rather than a permanent IAM
	// access key. STS hands back three values together — an access key ID, a
	// secret access key and a session token — so put the third one here.
	// Leave it empty for a permanent access key. Only used together with
	// AccessKeyID and SecretAccessKey. Optional.
	SessionToken string `json:"session_token"`

	// Profile is the name of a profile in the AWS shared configuration files
	// on the host running Tyk (~/.aws/config and ~/.aws/credentials) — the
	// same files and profile names the AWS CLI uses with `aws --profile
	// <name>`. Tyk then takes its credentials from that profile. Cannot be
	// combined with AccessKeyID and SecretAccessKey. Optional.
	Profile string `json:"profile"`

	// RoleARN is the ARN of an IAM role for Tyk to assume, for example
	// "arn:aws:iam::123456789012:role/tyk-secrets-reader". When it is set, Tyk
	// authenticates with the credentials described above, asks AWS STS for the
	// role, and then reads secrets with the role's permissions — so the role,
	// rather than the original identity, is what needs access to the secrets.
	// Use it to reach secrets held in a different AWS account, or to manage
	// everything Tyk is allowed to do in a single role. Leave it empty to use
	// the credentials directly. Optional.
	RoleARN string `json:"role_arn"`

	// ExternalID is a shared string that the owner of a role can require
	// callers to present, as an extra check that the caller really is who the
	// role expects. Where the role's trust policy asks for an external ID —
	// common when the role belongs to another account or to a third party —
	// enter the same value here, otherwise AWS refuses to grant the role. Only
	// used with RoleARN. Optional.
	ExternalID string `json:"external_id"`

	// RoleSessionName is a name of your choosing for Tyk's use of the role,
	// for example "tyk-gateway". AWS records it against every request Tyk
	// makes with that role, so it appears in CloudTrail logs and makes Tyk's
	// activity easy to tell apart from anything else using the same role. AWS
	// generates a name when this is empty. Only used with RoleARN. Optional.
	RoleSessionName string `json:"role_session_name"`

	// VersionStage makes the store read the version of a secret that carries
	// this staging label, rather than the one in use. Staging labels are names
	// AWS attaches to the versions of a secret: it maintains "AWSCURRENT" for
	// the version in use and "AWSPREVIOUS" for the one before it, and you can
	// add labels of your own. Leave it empty to read "AWSCURRENT", which is
	// what almost every store wants. Cannot be combined with VersionID.
	// Optional.
	//
	// With any label other than "AWSCURRENT" the store becomes read-only: Tyk
	// rejects attempts to write secrets through it.
	VersionStage string `json:"version_stage"`

	// VersionID makes the store read one exact, unchanging version of a
	// secret, named by the version ID AWS assigns to it. The value Tyk reads
	// then stays the same forever, even after the secret is rotated, so this
	// is for holding to a known value rather than for everyday use. Cannot be
	// combined with VersionStage. Optional.
	//
	// A store fixed to one version is read-only: Tyk rejects attempts to
	// write secrets through it.
	VersionID string `json:"version_id"`

	// Timeout is how long Tyk waits for a single Secrets Manager request —
	// reading or writing one secret — before giving up and reporting the store
	// as unavailable. Give it as a Go duration string: "5s", "500ms", "1m".
	// Defaults to 5s when omitted. Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline removes a single newline character from the end of
	// the value Tyk reads from a secret, if one is present. Secrets created
	// from the command line often pick up a trailing newline, which would
	// otherwise count as part of the value and break credentials such as
	// tokens and passwords. Defaults to false, which passes values on exactly
	// as stored. Optional.
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
