package gcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/kv"
)

const (
	// googleAPIDomain is the apex host for Google APIs. A WIF token_url and
	// service_account_impersonation_url must be this host or a subdomain
	// (covers sts., iamcredentials., and their regional forms).
	// Source: https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
	googleAPIDomain = "googleapis.com"

	// awsIMDSHostIPv4 and awsIMDSHostIPv6 are the AWS Instance Metadata Service
	// (IMDS) endpoints. An AWS-sourced WIF config must fetch credentials from
	// these, never an attacker-chosen host (SSRF).
	awsIMDSHostIPv4 = "169.254.169.254"
	awsIMDSHostIPv6 = "fd00:ec2::254"
)

var (
	allowedCredentialsTypes = []string{"service_account", "authorized_user", "external_account"}
	allowedTransports       = []string{"grpc", "rest"}
)

// FIX: Validate me
// Config is the JSON "config" block of a gcp_secret_manager store.
type Config struct {
	// ProjectID is the GCP project that owns the secrets. Required.
	ProjectID string `json:"project_id"`

	// QuotaProjectID sets the project billed/quota-attributed for API calls
	// (the x-goog-user-project header, via option.WithQuotaProject). Optional.
	QuotaProjectID string `json:"quota_project_id"`

	// Location, when set, targets REGIONAL Secret Manager (data residency).
	// Example: "europe-west1". When set, the client uses the regional endpoint
	// and all resource names include /locations/<Location>/. Optional.
	Location string `json:"location"`

	// CredentialsType describes how to interpret an EXPLICIT credential passed via
	// CredentialsFile/CredentialsJSON. It does NOT select an auth method.
	//   - ADC path (no CredentialsFile/JSON): the type is auto-detected from the ADC
	//     source (GOOGLE_APPLICATION_CREDENTIALS file, gcloud, or metadata server), so
	//     this field is irrelevant and must be empty. WIF works this way too — point
	//     GOOGLE_APPLICATION_CREDENTIALS at the WIF file and leave this empty.
	//   - Explicit path: set CredentialsFile or CredentialsJSON AND this field to one of
	//     "service_account" | "authorized_user" | "external_account".
	//
	// Setting it without any credential is a no-op mistake, so it is rejected.
	CredentialsType string `json:"credentials_type"`

	// CredentialsFile is a path to a credentials JSON file. Mutually exclusive
	// with CredentialsJSON. Optional.
	CredentialsFile string `json:"credentials_file"`

	// CredentialsJSON is the raw credentials JSON payload (e.g. injected via env).
	// Mutually exclusive with CredentialsFile. Optional.
	CredentialsJSON string `json:"credentials_json"`

	// ImpersonateServiceAccount, when set, makes the client fetch/write secrets AS
	// this target SA. The base identity is the explicit credentials above if
	// provided, otherwise ADC. The base identity needs
	// roles/iam.serviceAccountTokenCreator on the target. Optional.
	ImpersonateServiceAccount string `json:"impersonate_service_account"`

	// ImpersonateDelegates is chained-delegation path to reach the target
	// principal. Meaningful only with ImpersonateServiceAccount. Optional.
	ImpersonateDelegates []string `json:"impersonate_delegates"`

	// Timeout bounds each RPC. Go duration string ("5s", "500ms"). Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline, when true, strips a single trailing "\n" from the
	// payload. Guards the common footgun of secrets created with a trailing
	// newline (e.g. `echo val | gcloud secrets create`). Optional.
	TrimTrailingNewline bool `json:"trim_trailing_newline"`

	// Transport selects the wire protocol: "grpc" (default, recommended) or
	// "rest". Use "rest" only in networks hostile to gRPC/HTTP-2. Optional.
	Transport string `json:"transport"`
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

		return &gcpProvider{
			cfg:     &config,
			timeout: timeout,
		}, nil
	}
}

func parseConfig(raw json.RawMessage, config *Config) error {
	if len(raw) == 0 {
		return errors.New("gcp: config is missing")
	}

	if err := json.Unmarshal(raw, &config); err != nil {
		return fmt.Errorf("gcp: invalid config: %w", err)
	}

	return nil
}

func (cfg *Config) validate() error {
	if cfg.ProjectID == "" {
		return errors.New("gcp: project_id is required")
	}

	if err := cfg.validateCredentials(); err != nil {
		return err
	}

	if len(cfg.ImpersonateDelegates) > 0 && cfg.ImpersonateServiceAccount == "" {
		return errors.New("gcp: impersonate_delegates set without impersonate_service_account")
	}

	if cfg.Transport != "" && !slices.Contains(allowedTransports, cfg.Transport) {
		return fmt.Errorf(`gcp: unsupported transport %q (want "grpc" or "rest")`, cfg.Transport)
	}

	return nil
}

func (cfg *Config) validateCredentials() error {
	hasFile := cfg.CredentialsFile != ""
	hasJSON := cfg.CredentialsJSON != ""

	if hasFile && hasJSON {
		return errors.New("gcp: credentials_file and credentials_json are mutually exclusive")
	}

	if !hasFile && !hasJSON {
		if cfg.CredentialsType != "" {
			return errors.New("gcp: credentials_type set without credentials_file or credentials_json")
		}

		return nil
	}

	if cfg.CredentialsType == "" {
		return errors.New("gcp: credentials_type is required with credentials_file/credentials_json")
	}

	if !slices.Contains(allowedCredentialsTypes, cfg.CredentialsType) {
		return fmt.Errorf("gcp: unsupported or insecure credentials_type: %q", cfg.CredentialsType)
	}

	if cfg.CredentialsType == "external_account" {
		return cfg.validateExternalAccount()
	}

	return nil
}

func (cfg *Config) parsedTimeout() (time.Duration, error) {
	if cfg.Timeout != "" {
		d, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return 0, fmt.Errorf("gcp: invalid timeout %q: %w", cfg.Timeout, err)
		}

		if d > 0 {
			return d, nil
		}
	}

	return 0, nil
}

// validateExternalAccount checks a WIF (external_account) config before the SDK
// uses it — the SDK deliberately does not. A tampered token_url or AWS metadata
// URL is an exfiltration/SSRF vector and an executable source is RCE, so each is
// rejected against Google's expected-value table.
// Source: https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
func (cfg *Config) validateExternalAccount() error {
	raw, err := cfg.readExternalAccountJSON()
	if err != nil {
		return err
	}

	var ea externalAccount

	if err := json.Unmarshal(raw, &ea); err != nil {
		return fmt.Errorf("gcp: invalid external_account config: %w", err)
	}

	return ea.validate()
}

func (cfg *Config) readExternalAccountJSON() ([]byte, error) {
	raw := []byte(cfg.CredentialsJSON)

	if cfg.CredentialsFile != "" {
		b, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("gcp: read external_account credentials_file: %w", err)
		}

		raw = b
	}

	return raw, nil
}

type externalAccount struct {
	Type                           string           `json:"type"`
	TokenURL                       string           `json:"token_url"`
	ServiceAccountImpersonationURL string           `json:"service_account_impersonation_url"`
	CredentialSource               credentialSource `json:"credential_source"`
}

func (ea *externalAccount) validate() error {
	if ea.Type != "external_account" {
		return fmt.Errorf("gcp: external_account config type is %q, want external_account", ea.Type)
	}

	if ea.TokenURL != "" && !isGoogleHost(ea.TokenURL) {
		return fmt.Errorf("gcp: external_account token_url %q is not a Google endpoint", ea.TokenURL)
	}

	if ea.ServiceAccountImpersonationURL != "" && !isGoogleHost(ea.ServiceAccountImpersonationURL) {
		return fmt.Errorf(
			"gcp: external_account service_account_impersonation_url %q is not a Google endpoint",
			ea.ServiceAccountImpersonationURL,
		)
	}

	if len(ea.CredentialSource.Executable) > 0 {
		return errors.New("gcp: external_account executable credential source is not permitted")
	}

	return ea.CredentialSource.validateAWS()
}

type credentialSource struct {
	URL                   string          `json:"url"`
	Executable            json.RawMessage `json:"executable"`
	EnvironmentID         string          `json:"environment_id"`
	RegionURL             string          `json:"region_url"`
	IMDSv2SessionTokenURL string          `json:"imdsv2_session_token_url"`
}

func (cs *credentialSource) validateAWS() error {
	if !strings.HasPrefix(cs.EnvironmentID, "aws") {
		return nil
	}

	for _, u := range []string{cs.URL, cs.RegionURL, cs.IMDSv2SessionTokenURL} {
		if u != "" && !isAWSIMDSHost(u) {
			return fmt.Errorf("gcp: external_account aws credential source url %q is not the AWS IMDS endpoint", u)
		}
	}

	return nil
}

func isGoogleHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	return host == googleAPIDomain || strings.HasSuffix(host, "."+googleAPIDomain)
}

func isAWSIMDSHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	return host == awsIMDSHostIPv4 || host == awsIMDSHostIPv6
}
