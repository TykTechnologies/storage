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

// Config is used to configure a Google Cloud Secret Manager store.
type Config struct {
	// ProjectID is the ID of the Google Cloud project that holds the secrets, for
	// example "my-company-prod". Use the project ID, not the display name or the
	// numeric project number. A store reads from this one project only; to read
	// secrets from a second project, configure a second store. Required.
	ProjectID string `json:"project_id"`

	// QuotaProjectID names a different Google Cloud project to charge the API
	// requests to, for both billing and API quota. By default the requests count
	// against the project that owns the secrets. Set this only if your
	// organisation deliberately separates the two — the usual reason is a shared
	// secrets project whose quota should not be consumed by every service reading
	// from it. The identity Tyk uses needs the "serviceusage.services.use"
	// permission on the project named here. Optional.
	QuotaProjectID string `json:"quota_project_id"`

	// Location confines the store to a single Google Cloud region, for example
	// "europe-west1", by using Secret Manager's regional service rather than the
	// global one. Set it when the secrets were created as regional secrets — data
	// residency rules commonly require this — and leave it empty for ordinary
	// global secrets. It has to match how the secrets were created: a global
	// secret cannot be read through a regional store, or the reverse. Optional.
	Location string `json:"location"`

	// CredentialsType tells Tyk which kind of credential file it is being given,
	// and is only used together with CredentialsFile or CredentialsJSON. It does
	// not choose an authentication method — the presence of the credential does
	// that. One of:
	//   - "service_account"  a service-account key file, the usual choice when
	//                        running outside Google Cloud;
	//   - "authorized_user"  a user credential of the kind `gcloud auth login`
	//                        writes, intended for local development;
	//   - "external_account" a Workload Identity Federation file, which lets an
	//                        identity from another provider (AWS, Azure, any
	//                        OIDC issuer) act as a Google service account
	//                        without a long-lived key.
	//
	// Required when a credential is supplied, and must be left empty when none
	// is: with no credential Tyk uses Application Default Credentials instead
	// (see CredentialsFile), which recognise their own type, so a value here
	// would do nothing and is rejected as a likely mistake. Any other value is
	// rejected as well. Optional.
	//
	// A credential declared as "external_account" is inspected more closely than
	// the others. Unlike a key file, a federation file does not contain a
	// credential — it describes where to go and get one — so an altered file can
	// redirect that exchange somewhere of the author's choosing. Tyk therefore
	// requires the Google addresses inside it to be genuine Google endpoints, and
	// requires a file that federates from AWS to read from the standard AWS
	// metadata address.
	//
	// One kind of federation file is refused outright. Google allows the file to
	// obtain its token by running a program that the file itself names, known as
	// an executable-sourced credential; that would let whoever supplies the file
	// choose what Tyk runs on its host, so Tyk rejects it and the store does not
	// start. If your federation file is of that kind, replace it with one that
	// reads the token from a path on disk or fetches it from a URL — the forms
	// used by Kubernetes, AWS and Azure federation.
	CredentialsType string `json:"credentials_type"`

	// CredentialsFile is the path to a Google Cloud credentials JSON file on the
	// host running Tyk. Set it together with CredentialsType. Cannot be combined
	// with CredentialsJSON.
	//
	// Both are optional, and leaving them empty is the recommended setup on
	// Google Cloud: Tyk then uses Application Default Credentials, Google's
	// standard way for an application to find credentials from its surroundings —
	// the GOOGLE_APPLICATION_CREDENTIALS environment variable, the credentials
	// left by `gcloud auth login`, or the service account attached to the GKE
	// workload, Cloud Run service or Compute Engine instance Tyk runs on. No
	// secret material then has to be stored in Tyk's own configuration.
	//
	// Whichever identity is used needs read access to the secrets, which on
	// Google Cloud means the "roles/secretmanager.secretAccessor" role. Writing
	// secrets through Tyk needs more: permission to add a secret version, and to
	// create a secret the first time one is written.
	CredentialsFile string `json:"credentials_file"`

	// CredentialsJSON is the content of a Google Cloud credentials JSON file,
	// supplied inline instead of as a file on disk — useful when the credential
	// arrives through an environment variable or a mounted Kubernetes Secret.
	// Set it together with CredentialsType. Cannot be combined with
	// CredentialsFile. Optional; see CredentialsFile for what applies when both
	// are empty.
	CredentialsJSON string `json:"credentials_json"`

	// ImpersonateServiceAccount is the email address of a service account for Tyk
	// to act as, for example "tyk-secrets@my-project.iam.gserviceaccount.com".
	// When set, Tyk authenticates with the credentials described above, then asks
	// Google for short-lived credentials for this service account and reads
	// secrets as it — so this service account, rather than the original identity,
	// is the one that needs access to the secrets. The original identity needs
	// the "roles/iam.serviceAccountTokenCreator" role on it.
	//
	// Use it to reach secrets in another project without moving credentials
	// around, or to keep the permissions granted to Tyk in one service account.
	// Leave it empty to read as the identity Tyk authenticated with. Optional.
	ImpersonateServiceAccount string `json:"impersonate_service_account"`

	// ImpersonateDelegates is for the uncommon case where the identity Tyk
	// authenticates with may not act as the target service account directly, but
	// reaches it through intermediate service accounts. List their email
	// addresses in order, from the one Tyk's own identity is allowed to act as
	// through to the one allowed to act as the target. Each must hold the
	// "roles/iam.serviceAccountTokenCreator" role on the next.
	//
	// Leave it empty unless your organisation has deliberately set up such a
	// chain. Optional, but only alongside ImpersonateServiceAccount: with no
	// service account to reach, a chain leads nowhere, so on its own it is
	// rejected when the store starts.
	ImpersonateDelegates []string `json:"impersonate_delegates"`

	// Timeout is how long Tyk waits for a single Secret Manager request — reading
	// or writing one secret — before giving up and reporting the store as
	// unavailable. Give it as a Go duration string: "5s", "500ms", "1m".
	// Defaults to 5s when omitted; a value Tyk cannot read as a duration stops
	// the store from starting. Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline removes a single newline character from the end of the
	// value Tyk reads from a secret, if one is present. Secrets created from the
	// command line often pick up a trailing newline, which would otherwise count
	// as part of the value and break credentials such as tokens and passwords.
	// Defaults to false, which passes values on exactly as stored. Optional.
	TrimTrailingNewline bool `json:"trim_trailing_newline"`

	// Transport is the protocol Tyk uses to talk to Secret Manager: "grpc" or
	// "rest". Defaults to "grpc", which is faster and is what Google's own
	// libraries use. Switch to "rest" only if the network between Tyk and Google
	// cannot carry gRPC — some older proxies and firewalls interfere with the
	// HTTP/2 connections it depends on. Any other value is rejected when the
	// store starts. Optional.
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
