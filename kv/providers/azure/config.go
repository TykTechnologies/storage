package azure

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/TykTechnologies/storage/kv"
)

const defaultCredentialType = "managed_identity"

// guidRe matches the 36-char canonical UUID.
var guidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Config is used to configure an Azure Key Vault store.
type Config struct {
	// VaultURL is the address of the key vault holding the secrets, in the form
	// "https://<vault-name>.vault.azure.net" — Azure shows it as the vault's "Vault
	// URI" in the portal. Give the bare address with no path or query. Only the
	// global Azure cloud is supported: the vaults of the sovereign clouds
	// (Azure US Government, Azure China) are rejected. Required.
	VaultURL string `json:"vault_url"`

	// CredentialType is how Tyk authenticates to the key vault. Azure offers
	// several mechanisms and they need different fields below, so this choice
	// decides which of them apply. One of:
	//   - "managed_identity"    an identity Azure attaches to the resource Tyk
	//                           runs on (a VM, App Service, Container App, AKS
	//                           pod), with no credential kept by Tyk at all.
	//                           This is the default and the recommended choice
	//                           whenever Tyk runs inside Azure. See ClientID.
	//   - "workload_identity"   for Tyk running in Kubernetes: the cluster places
	//                           a short-lived token in a file that Microsoft
	//                           Entra ID is configured to trust. Needs TenantID,
	//                           ClientID and FederatedTokenFile.
	//   - "client_secret"       an app registration and its secret, the usual
	//                           choice when Tyk runs outside Azure. Needs
	//                           TenantID, ClientID and ClientSecret.
	//   - "client_certificate"  an app registration that proves itself with a
	//                           certificate instead of a secret — longer-lived
	//                           and not a password to leak. Needs TenantID,
	//                           ClientID and ClientCertificateFile.
	//
	// Defaults to "managed_identity" when omitted, and any value other than the
	// four above is rejected when the store starts. Optional.
	//
	// Whichever is chosen, the identity needs permission to read the secrets:
	// the "Key Vault Secrets User" role when the vault uses Azure role-based
	// access control, or a "get" secret permission when it uses the older vault
	// access policies. Writing secrets through Tyk needs "Key Vault Secrets
	// Officer" or a "set" permission.
	CredentialType string `json:"credential_type"`

	// TenantID identifies the Microsoft Entra ID directory the identity belongs
	// to, as a GUID — Azure shows it as "Directory (tenant) ID" on an app
	// registration's overview page. Required for the "client_secret",
	// "client_certificate" and "workload_identity" credential types; ignored for
	// "managed_identity", where Azure already knows the directory.
	TenantID string `json:"tenant_id"`

	// ClientID identifies the application or identity Tyk authenticates as, as a
	// GUID — Azure shows it as "Application (client) ID" on an app registration,
	// or as "Client ID" on a managed identity.
	//
	// Required for the "client_secret", "client_certificate" and
	// "workload_identity" credential types. For "managed_identity" it is
	// optional and selects which identity to use: give the client ID of a
	// user-assigned identity, or leave it empty to use the system-assigned
	// identity of the Azure resource the component runs on. Fill it in whenever
	// more than one user-assigned identity is attached, since Azure cannot then
	// choose for you.
	ClientID string `json:"client_id"`

	// ClientSecret is the secret value of the app registration named by ClientID
	// — the string Azure shows exactly once, when the secret is created.
	// Use the value, not the secret's ID or name.
	// Required for the "client_secret" credential type, ignored otherwise.
	//
	// Client secrets expire, and Azure will not warn Tyk in advance: an expired
	// secret shows up as reads suddenly failing. Prefer "managed_identity" or
	// "workload_identity" where the environment allows it.
	ClientSecret string `json:"client_secret"`

	// ClientCertificateFile is the path to the certificate file the app
	// registration authenticates with, on the host running the Tyk component.
	// The file must hold both the certificate and its private key, as PEM or PKCS#12
	// (a .pfx or .p12 file) — the same certificate that was uploaded to the app
	// registration in Azure. Required for the "client_certificate" credential
	// type, ignored otherwise.
	//
	// The file is read as the store starts, so a path that is missing, unreadable
	// or not a certificate Tyk can parse stops the store from starting rather
	// than surfacing later as a failed read.
	ClientCertificateFile string `json:"client_certificate_file"`

	// ClientCertificatePassword is the password protecting the PKCS#12
	// certificate file, if it has one. Leave it empty for an unprotected file or
	// for PEM. Optional.
	//
	// Two limitations come from the Azure SDK: a PEM file whose private key is
	// itself encrypted cannot be opened at all, whatever is set here, and neither
	// can a PKCS#12 file that uses SHA-256 to authenticate its contents. In both
	// cases convert the file — an unencrypted PEM is the reliable form.
	ClientCertificatePassword string `json:"client_certificate_password"`

	// FederatedTokenFile is the path to a file holding the short-lived token that
	// proves Tyk's identity to Microsoft Entra ID. Kubernetes writes such a file
	// into the pod for the workload's service account, and Entra ID is configured
	// to trust it against an app registration.
	//
	// On AKS with workload identity enabled the file is provided automatically and
	// its path is in the AZURE_FEDERATED_TOKEN_FILE environment variable. This is
	// not limited to AKS: any Kubernetes cluster — EKS, GKE, self-hosted — can
	// project such a token, as can any other source that writes a trusted OpenID
	// Connect token to a file. Required for the "workload_identity" credential
	// type, ignored otherwise.
	FederatedTokenFile string `json:"federated_token_file"`

	// Timeout is how long Tyk waits for a single Key Vault request — reading or
	// writing one secret — before giving up and reporting the store as
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
			timeout:             timeout,
			client:              client,
			trimTrailingNewline: config.TrimTrailingNewline,
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

	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return fmt.Errorf("azure: vault_url must be a bare host with no path or query, got %q", cfg.VaultURL)
	}

	if isSovereignVaultHost(u.Host) {
		return fmt.Errorf(
			"azure: vault_url %q targets a sovereign cloud but only Azure Public is supported",
			cfg.VaultURL)
	}

	return nil
}

func isSovereignVaultHost(host string) bool {
	host = strings.ToLower(host)

	return strings.HasSuffix(host, ".vault.usgovcloudapi.net") ||
		strings.HasSuffix(host, ".vault.azure.cn")
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
	cred, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ID: cfg.managedIdentityID(),
	})
	if err != nil {
		return nil, fmt.Errorf("azure: build managed_identity credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) managedIdentityID() azidentity.ManagedIDKind {
	if cfg.ClientID == "" {
		return nil
	}

	return azidentity.ClientID(cfg.ClientID)
}

func (cfg *Config) workloadIdentityCredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewWorkloadIdentityCredential(cfg.workloadIdentityOptions())
	if err != nil {
		return nil, fmt.Errorf("azure: build workload_identity credential: %w", err)
	}

	return cred, nil
}

func (cfg *Config) workloadIdentityOptions() *azidentity.WorkloadIdentityCredentialOptions {
	return &azidentity.WorkloadIdentityCredentialOptions{
		TenantID:      cfg.TenantID,
		ClientID:      cfg.ClientID,
		TokenFilePath: cfg.FederatedTokenFile,
	}
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
