package azure

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/TykTechnologies/storage/kv"
)

const defaultCredentialType = "managed_identity"

// guidRe matches the 36-char canonical UUID.
var guidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Config is the JSON "config" block of an azure_key_vault store.
type Config struct {
	// VaultURL is the Key Vault data-plane URL. Required.
	VaultURL string `json:"vault_url"`

	// CredentialType selects how the provider authenticates.
	// "managed_identity" is applied by default. Allowed:
	// "managed_identity" | "workload_identity" | "client_secret" | "client_certificate".
	CredentialType string `json:"credential_type"`

	// TenantID is the Entra tenant GUID. Required for client_secret, client_certificate,
	// and workload_identity; ignored for managed_identity.
	TenantID string `json:"tenant_id"`

	// ClientID is the app-registration / service-principal client GUID for the SP and
	// workload-identity modes. For managed_identity it selects a user-assigned identity by
	// its client ID or system-assigned if client ID is empty.
	ClientID string `json:"client_id"`

	// ClientSecret is the service-principal secret. Required for client_secret.
	ClientSecret string `json:"client_secret"`

	// ClientCertificateFile is a path to a PEM or PKCS#12 file (certificate + private key).
	// Required for client_certificate.
	ClientCertificateFile string `json:"client_certificate_file"`

	// ClientCertificatePassword decrypts a password-protected PKCS#12 certificate file.
	// LIMITATION: the Azure SDK cannot decrypt encrypted-PEM private keys at all,
	// and cannot open a PKCS#12 that uses SHA-256 for message authentication. Optional.
	ClientCertificatePassword string `json:"client_certificate_password"`

	// FederatedTokenFile is the path to a file holding an OIDC token that Entra trusts via
	// a federated identity credential. Required for workload_identity. Works for ANY
	// Kubernetes cluster (AKS injects it; EKS/GKE/on-prem project a service-account token)
	// and any file-sourced OIDC token — not AKS-only.
	FederatedTokenFile string `json:"federated_token_file"`

	// Timeout bounds each API call. Go duration string ("5s", "500ms"). Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline, when true, strips a single trailing "\n" from the value returned
	// by Get. Optional.
	TrimTrailingNewline bool `json:"trim_trailing_newline"`
}

// NewFactory builds an azure_key_vault provider from raw JSON config. It validates fully
// and builds the credential + secrets client in the factory.
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

		cred, err := config.credential()
		if err != nil {
			return nil, err
		}

		client, err := config.newClient(cred)
		if err != nil {
			return nil, err
		}

		return &azureProvider{
			cfg:     &config,
			timeout: timeout,
			client:  client,
		}, nil
	}
}

func parseConfig(raw json.RawMessage, cfg *Config) error {
	if len(raw) == 0 {
		return errors.New("azure: config is missing")
	}

	if err := json.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("azure: invalid config: %w", err)
	}

	return nil
}

func (cfg *Config) validate() error {
	if err := cfg.validateVaultURL(); err != nil {
		return err
	}

	return cfg.validateCredential()
}

func (cfg *Config) validateVaultURL() error {
	if cfg.VaultURL == "" {
		return errors.New("azure: vault_url is required")
	}

	u, err := url.Parse(cfg.VaultURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return errors.New("azure: vault_url must be an https URL")
	}

	return nil
}

func (cfg *Config) validateCredential() error {
	ct := cfg.effectiveCredentialType()

	switch ct {
	case "managed_identity":
		return cfg.validateManagedIdentity()
	case "workload_identity":
		return cfg.validateWorkloadIdentity()
	case "client_secret":
		return cfg.validateClientSecret()
	case "client_certificate":
		return cfg.validateClientCertificate()
	default:
		return fmt.Errorf("azure: unsupported or insecure credential_type %q", ct)
	}
}

func (cfg *Config) validateManagedIdentity() error {
	if cfg.ClientID == "" {
		return nil
	}

	return requireGUID("client_id", cfg.ClientID)
}

func (cfg *Config) validateWorkloadIdentity() error {
	if err := cfg.requireTenantAndClient(); err != nil {
		return err
	}

	if cfg.FederatedTokenFile == "" {
		return errors.New("azure: federated_token_file is required for the workload_identity credential_type")
	}

	return nil
}

func (cfg *Config) validateClientSecret() error {
	if err := cfg.requireTenantAndClient(); err != nil {
		return err
	}

	if cfg.ClientSecret == "" {
		return errors.New("azure: client_secret is required for the client_secret credential_type")
	}

	return nil
}

func (cfg *Config) validateClientCertificate() error {
	if err := cfg.requireTenantAndClient(); err != nil {
		return err
	}

	if cfg.ClientCertificateFile == "" {
		return errors.New("azure: client_certificate_file is required for the client_certificate credential_type")
	}

	return nil
}

func (cfg *Config) requireTenantAndClient() error {
	if err := requireGUID("tenant_id", cfg.TenantID); err != nil {
		return err
	}

	return requireGUID("client_id", cfg.ClientID)
}

func (cfg *Config) parsedTimeout() (time.Duration, error) {
	if cfg.Timeout == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return 0, fmt.Errorf("azure: invalid timeout %q: %w", cfg.Timeout, err)
	}

	if d <= 0 {
		return 0, nil
	}

	return d, nil
}

func (cfg *Config) credential() (azcore.TokenCredential, error) {
	switch cfg.effectiveCredentialType() {
	case "managed_identity":
		return cfg.managedIdentityCredential()
	case "workload_identity":
		return cfg.workloadIdentityCredential()
	case "client_secret":
		return cfg.clientSecretCredential()
	case "client_certificate":
		return cfg.clientCertificateCredential()
	default:
		return nil, fmt.Errorf("azure: unsupported credential_type %q", cfg.CredentialType)
	}
}

func (cfg *Config) managedIdentityCredential() (azcore.TokenCredential, error) {
	opts := &azidentity.ManagedIdentityCredentialOptions{}
	// System-assigned if not provided
	if cfg.ClientID != "" {
		opts.ID = azidentity.ClientID(cfg.ClientID)
	}

	cred, err := azidentity.NewManagedIdentityCredential(opts)
	if err != nil {
		return nil, fmt.Errorf("azure: build managed_identity credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) workloadIdentityCredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		TenantID:      cfg.TenantID,
		ClientID:      cfg.ClientID,
		TokenFilePath: cfg.FederatedTokenFile,
	})
	if err != nil {
		return nil, fmt.Errorf("azure: build workload_identity credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) clientSecretCredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: build client_secret credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) clientCertificateCredential() (azcore.TokenCredential, error) {
	data, err := os.ReadFile(cfg.ClientCertificateFile)
	if err != nil {
		return nil, fmt.Errorf("azure: read client_certificate_file: %w", err)
	}

	var pwd []byte
	if cfg.ClientCertificatePassword != "" {
		pwd = []byte(cfg.ClientCertificatePassword)
	}

	certs, key, err := azidentity.ParseCertificates(data, pwd)
	if err != nil {
		return nil, fmt.Errorf("azure: parse client_certificate_file: %w", err)
	}

	cred, err := azidentity.NewClientCertificateCredential(cfg.TenantID, cfg.ClientID, certs, key, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: build client_certificate credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) newClient(cred azcore.TokenCredential) (secretsClient, error) {
	client, err := azsecrets.NewClient(cfg.VaultURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: create client: %w", err)
	}

	return client, nil
}

func (cfg *Config) effectiveCredentialType() string {
	if cfg.CredentialType == "" {
		return defaultCredentialType
	}

	return cfg.CredentialType
}

// requireGUID enforces a present, canonical-GUID value for a required ID field.
func requireGUID(field, val string) error {
	if val == "" {
		return fmt.Errorf("azure: %s is required", field)
	}

	if !guidRe.MatchString(val) {
		return fmt.Errorf("azure: %s must be a GUID", field)
	}

	return nil
}
