package azure_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/azure"
	"github.com/stretchr/testify/require"
)

const (
	validTenantID = "11111111-1111-1111-1111-111111111111"
	validClientID = "22222222-2222-2222-2222-222222222222"
	validVaultURL = "https://myvault.vault.azure.net/"
)

func TestNewFactory_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      string
		wantErr     bool
		errContains string
	}{
		// --- invalid: rejected at construction ---
		{
			name:        "absent config",
			config:      "",
			wantErr:     true,
			errContains: "config is missing",
		},
		{
			name:        "malformed json",
			config:      `{"vault_url":`,
			wantErr:     true,
			errContains: "invalid config",
		},
		{
			name:        "missing vault_url",
			config:      cfgJSON(&azure.Config{}),
			wantErr:     true,
			errContains: "vault_url",
		},
		{
			name:        "non-https vault_url",
			config:      cfgJSON(&azure.Config{VaultURL: "http://myvault.vault.azure.net/"}),
			wantErr:     true,
			errContains: "https",
		},
		{
			name:        "vault_url without host",
			config:      cfgJSON(&azure.Config{VaultURL: "https://"}),
			wantErr:     true,
			errContains: "https",
		},
		{
			name:        "vault_url not a url",
			config:      cfgJSON(&azure.Config{VaultURL: "://nope"}),
			wantErr:     true,
			errContains: "vault_url",
		},
		{
			name:        "vault_url with a path rejected",
			config:      cfgJSON(&azure.Config{VaultURL: "https://myvault.vault.azure.net/secrets/foo"}),
			wantErr:     true,
			errContains: "path or query",
		},
		{
			name:        "vault_url with a query rejected",
			config:      cfgJSON(&azure.Config{VaultURL: "https://myvault.vault.azure.net/?api-version=1"}),
			wantErr:     true,
			errContains: "path or query",
		},
		{
			name:        "government cloud vault_url rejected",
			config:      cfgJSON(&azure.Config{VaultURL: "https://myvault.vault.usgovcloudapi.net"}),
			wantErr:     true,
			errContains: "sovereign",
		},
		{
			name:        "china cloud vault_url rejected",
			config:      cfgJSON(&azure.Config{VaultURL: "https://myvault.vault.azure.cn"}),
			wantErr:     true,
			errContains: "sovereign",
		},
		{
			name:   "vault_url without trailing slash accepted",
			config: cfgJSON(&azure.Config{VaultURL: "https://myvault.vault.azure.net"}),
		},
		{
			name:        "unsupported credential_type",
			config:      cfgJSON(&azure.Config{VaultURL: validVaultURL, CredentialType: "bogus"}),
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "rejected default credential_type",
			config:      cfgJSON(&azure.Config{VaultURL: validVaultURL, CredentialType: "default"}),
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "rejected cli credential_type",
			config:      cfgJSON(&azure.Config{VaultURL: validVaultURL, CredentialType: "cli"}),
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "rejected environment credential_type",
			config:      cfgJSON(&azure.Config{VaultURL: validVaultURL, CredentialType: "environment"}),
			wantErr:     true,
			errContains: "unsupported",
		},

		// --- client_secret required fields ---
		{
			name: "client_secret missing tenant_id",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				ClientID:       validClientID,
				ClientSecret:   "s",
			}),
			wantErr:     true,
			errContains: "tenant_id",
		},
		{
			name: "client_secret missing client_id",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				TenantID:       validTenantID,
				ClientSecret:   "s",
			}),
			wantErr:     true,
			errContains: "client_id",
		},
		{
			name: "client_secret missing client_secret",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				TenantID:       validTenantID,
				ClientID:       validClientID,
			}),
			wantErr:     true,
			errContains: "client_secret",
		},
		{
			name: "client_secret non-guid tenant_id",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				TenantID:       "not-a-guid",
				ClientID:       validClientID,
				ClientSecret:   "s",
			}),
			wantErr:     true,
			errContains: "tenant_id",
		},
		{
			name: "client_secret tenant alias common rejected",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				TenantID:       "common",
				ClientID:       validClientID,
				ClientSecret:   "s",
			}),
			wantErr:     true,
			errContains: "tenant_id",
		},

		// --- client_certificate required fields (file cases are separate) ---
		{
			name: "client_certificate missing certificate file",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_certificate",
				TenantID:       validTenantID,
				ClientID:       validClientID,
			}),
			wantErr:     true,
			errContains: "client_certificate_file",
		},
		{
			name: "client_certificate missing tenant_id",
			config: cfgJSON(&azure.Config{
				VaultURL:              validVaultURL,
				CredentialType:        "client_certificate",
				ClientID:              validClientID,
				ClientCertificateFile: "/some/cert.pem",
			}),
			wantErr:     true,
			errContains: "tenant_id",
		},

		// --- workload_identity required fields ---
		{
			name: "workload_identity missing federated_token_file",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "workload_identity",
				TenantID:       validTenantID,
				ClientID:       validClientID,
			}),
			wantErr:     true,
			errContains: "federated_token_file",
		},
		{
			name: "workload_identity missing tenant_id",
			config: cfgJSON(&azure.Config{
				VaultURL:           validVaultURL,
				CredentialType:     "workload_identity",
				ClientID:           validClientID,
				FederatedTokenFile: "/var/run/token",
			}),
			wantErr:     true,
			errContains: "tenant_id",
		},

		// --- managed_identity user-assigned GUID guard ---
		{
			name: "managed_identity non-guid client_id",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "managed_identity",
				ClientID:       "not-a-guid",
			}),
			wantErr:     true,
			errContains: "client_id",
		},

		// --- timeout ---
		{
			name:        "unparseable timeout",
			config:      cfgJSON(&azure.Config{VaultURL: validVaultURL, Timeout: "5 seconds"}),
			wantErr:     true,
			errContains: "timeout",
		},

		// --- valid: factory returns a provider, no error ---
		{
			name:   "managed_identity default (vault_url only)",
			config: cfgJSON(&azure.Config{VaultURL: validVaultURL}),
		},
		{
			name:   "managed_identity explicit",
			config: cfgJSON(&azure.Config{VaultURL: validVaultURL, CredentialType: "managed_identity"}),
		},
		{
			name: "managed_identity user-assigned (guid client_id)",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "managed_identity",
				ClientID:       validClientID,
			}),
		},
		{
			name: "workload_identity",
			config: cfgJSON(&azure.Config{
				VaultURL:           validVaultURL,
				CredentialType:     "workload_identity",
				TenantID:           validTenantID,
				ClientID:           validClientID,
				FederatedTokenFile: "/var/run/secrets/azure/tokens/token",
			}),
		},
		{
			name: "client_secret",
			config: cfgJSON(&azure.Config{
				VaultURL:       validVaultURL,
				CredentialType: "client_secret",
				TenantID:       validTenantID,
				ClientID:       validClientID,
				ClientSecret:   "s3cr3t",
			}),
		},
		{
			name:   "valid timeout",
			config: cfgJSON(&azure.Config{VaultURL: validVaultURL, Timeout: "7s"}),
		},
		{
			name:   "trim_trailing_newline",
			config: cfgJSON(&azure.Config{VaultURL: validVaultURL, TrimTrailingNewline: true}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider, err := azure.NewFactory()(json.RawMessage(tt.config))

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errContains)
				require.Nil(t, provider)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, provider)
		})
	}
}

func TestNewFactory_ClientCertificate(t *testing.T) {
	t.Parallel()

	certConfig := func(path string) string {
		return cfgJSON(&azure.Config{
			VaultURL:              validVaultURL,
			CredentialType:        "client_certificate",
			TenantID:              validTenantID,
			ClientID:              validClientID,
			ClientCertificateFile: path,
		})
	}

	t.Run("valid pem cert+key builds a provider", func(t *testing.T) {
		t.Parallel()

		path := writeValidCertPEM(t)
		provider, err := azure.NewFactory()(json.RawMessage(certConfig(path)))
		require.NoError(t, err)
		require.NotNil(t, provider)
	})

	t.Run("unreadable file fails loud", func(t *testing.T) {
		t.Parallel()

		missing := filepath.Join(t.TempDir(), "does-not-exist.pem")
		provider, err := azure.NewFactory()(json.RawMessage(certConfig(missing)))
		require.Error(t, err)
		require.ErrorContains(t, err, "client_certificate_file")
		require.Nil(t, provider)
	})

	t.Run("malformed file fails loud", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "garbage.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))

		provider, err := azure.NewFactory()(json.RawMessage(certConfig(path)))
		require.Error(t, err)
		require.ErrorContains(t, err, "parse")
		require.Nil(t, provider)
	})
}

func TestNewFactory_TimeoutExposedViaTimeouter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		config string
		want   time.Duration
	}{
		{"parsed positive timeout", `{"vault_url":"` + validVaultURL + `","timeout":"7s"}`, 7 * time.Second},
		{"negative timeout is zero", `{"vault_url":"` + validVaultURL + `","timeout":"-10s"}`, 0},
		{"absent timeout is zero", `{"vault_url":"` + validVaultURL + `"}`, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider, err := azure.NewFactory()(json.RawMessage(tc.config))
			require.NoError(t, err)

			timeouter, ok := kv.AsTimeouter(provider)
			require.True(t, ok, "azure provider must implement Timeouter")
			require.Equal(t, tc.want, timeouter.Timeout())
		})
	}
}

func TestProvider_InterfaceContract(t *testing.T) {
	t.Parallel()

	provider, err := azure.NewFactory()(json.RawMessage(cfgJSON(&azure.Config{VaultURL: validVaultURL})))
	require.NoError(t, err)

	_, isSetter := kv.AsSetter(provider)
	require.True(t, isSetter, "must implement Setter")

	_, isTimeouter := kv.AsTimeouter(provider)
	require.True(t, isTimeouter, "must implement Timeouter")

	_, isStandalone := kv.AsStandaloner(provider)
	require.False(t, isStandalone, "must NOT implement Standalone (stays cache/singleflight-wrapped)")

	_, isInitializer := kv.AsInitializer(provider)
	require.False(t, isInitializer, "must NOT implement Initializer (client built in factory, D5)")

	_, isCloser := kv.AsCloser(provider)
	require.False(t, isCloser, "must NOT implement Closer (no resource to release, D5)")
}

func cfgJSON(c *azure.Config) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func writeValidCertPEM(t *testing.T) string {
	t.Helper()

	// RSA, not ECDSA: azidentity's ClientCertificateCredential signs the Entra client
	// assertion with an RSA key and rejects non-RSA keys ("key must be an RSA key").
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "azure-provider-test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31-1, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, pem.Encode(&buf, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))

	path := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))

	return path
}
