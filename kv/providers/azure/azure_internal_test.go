package azure

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestValidateSecretKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         string
		wantName    string
		wantVersion string
		wantErr     bool
		errContains string
	}{
		// --- accepted ---
		{name: "bare name", key: "db-password", wantName: "db-password"},
		{name: "name with version", key: "db-password/a1b2c3d4", wantName: "db-password", wantVersion: "a1b2c3d4"},
		{name: "mixed-case name and version", key: "AbC1/Ff09", wantName: "AbC1", wantVersion: "Ff09"},

		// --- rejected ---
		{name: "empty key", key: "", wantErr: true, errContains: "empty secret key"},
		{name: "full url", key: "https://other.vault.azure.net/secret", wantErr: true, errContains: "bare secret name"},
		{name: "underscore in name", key: "my_secret", wantErr: true, errContains: "bare secret name"},
		{name: "dot in name", key: "my.secret", wantErr: true, errContains: "bare secret name"},
		{name: "path traversal name", key: "../etc", wantErr: true, errContains: "bare secret name"},
		{name: "too many segments", key: "a/b/c", wantErr: true, errContains: "too many path segments"},
		{name: "invalid version chars", key: "name/ver$ion", wantErr: true, errContains: "not a valid version identifier"},
		{name: "version path traversal", key: "name/..", wantErr: true, errContains: "not a valid version identifier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, version, err := validateSecretKey(tt.key)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errContains)
				require.Empty(t, name)
				require.Empty(t, version)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantName, name)
			require.Equal(t, tt.wantVersion, version)
		})
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()

	const key = "db-password"

	p := &azureProvider{}

	t.Run("404 SecretNotFound -> KeyNotFoundError", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusNotFound, "SecretNotFound", "", ""))

		var knf *kv.KeyNotFoundError
		require.ErrorAs(t, err, &knf)
	})

	t.Run("403 SecretDisabled -> plain, not typed", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusForbidden, "Forbidden", "SecretDisabled", ""))
		require.ErrorContains(t, err, "disabled")
		requireNotTyped(t, err)
	})

	t.Run("400 BadParameter -> plain, not typed", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusBadRequest, "BadParameter", "", ""))
		require.ErrorContains(t, err, "invalid")
		requireNotTyped(t, err)
	})

	t.Run("409 ObjectIsDeletedButRecoverable -> plain, not typed", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusConflict, "Conflict", "ObjectIsDeletedButRecoverable", ""))
		require.ErrorContains(t, err, "soft-deleted")
		requireNotTyped(t, err)
	})

	t.Run("401 -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		requireStoreUnavailable(t, p.classify(key, newResponseError(http.StatusUnauthorized, "Unauthorized", "", "")))
	})

	t.Run("403 access denied (no inner code) -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		requireStoreUnavailable(t, p.classify(key, newResponseError(http.StatusForbidden, "Forbidden", "", "")))
	})

	t.Run("429 -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		requireStoreUnavailable(t, p.classify(key, newResponseError(http.StatusTooManyRequests, "Throttled", "", "")))
	})

	t.Run("500 -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusInternalServerError, "InternalError", "", ""))
		requireStoreUnavailable(t, err)
	})

	t.Run("non-HTTP error -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		requireStoreUnavailable(t, p.classify(key, errors.New("dial tcp: i/o timeout")))
	})

	t.Run("x-ms-request-id surfaced in the transient error", func(t *testing.T) {
		t.Parallel()

		err := p.classify(key, newResponseError(http.StatusInternalServerError, "InternalError", "", "req-abc123"))
		require.ErrorContains(t, err, "req-abc123")
	})
}

func TestGet(t *testing.T) {
	t.Parallel()

	t.Run("valid value returned verbatim", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("hunter2")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		got, err := p.Get(context.Background(), "db-password")
		require.NoError(t, err)
		require.Equal(t, "hunter2", got)
	})

	t.Run("empty value is a successful empty string", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		got, err := p.Get(context.Background(), "k")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("nil value -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: nil}}}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "k")
		requireStoreUnavailable(t, err)
	})

	t.Run("pinned version is passed to the client", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("v")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "db-password/abc123")
		require.NoError(t, err)
		require.Equal(t, "db-password", fake.gotGetName)
		require.Equal(t, "abc123", fake.gotGetVersion)
	})

	t.Run("bare name passes empty version", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("v")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "db-password")
		require.NoError(t, err)
		require.Empty(t, fake.gotGetVersion)
	})

	t.Run("invalid key rejected before any client call", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "https://evil.vault.azure.net/x")
		require.Error(t, err)
		require.Empty(t, fake.gotGetName)
	})

	t.Run("empty key rejected before any client call", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "")
		require.ErrorContains(t, err, "empty secret key")
		require.Empty(t, fake.gotGetName)
	})

	t.Run("trim_trailing_newline strips exactly one newline", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("token\n\n")}
		p := &azureProvider{client: fake, cfg: &Config{TrimTrailingNewline: true}}

		got, err := p.Get(context.Background(), "k")
		require.NoError(t, err)
		require.Equal(t, "token\n", got)
	})

	t.Run("trim off preserves the newline", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("token\n")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		got, err := p.Get(context.Background(), "k")
		require.NoError(t, err)
		require.Equal(t, "token\n", got)
	})

	t.Run("routes call errors through classify (404 -> KeyNotFoundError)", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getErr: newResponseError(http.StatusNotFound, "SecretNotFound", "", "")}
		p := &azureProvider{client: fake, cfg: &Config{}}

		_, err := p.Get(context.Background(), "missing")

		var knf *kv.KeyNotFoundError
		require.ErrorAs(t, err, &knf)
	})
}

func TestSet(t *testing.T) {
	t.Parallel()

	t.Run("upsert writes the value in a single call", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		p := &azureProvider{client: fake, cfg: &Config{}}

		err := p.Set(context.Background(), "db-password", "s3cr3t")
		require.NoError(t, err)
		require.Equal(t, "db-password", fake.gotSetName)
		require.Equal(t, "s3cr3t", fake.gotSetValue)
		require.Equal(t, 1, fake.setCalls) // no create-on-missing dance (D11)
	})

	t.Run("version-pinned key rejected before any client call", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		p := &azureProvider{client: fake, cfg: &Config{}}

		err := p.Set(context.Background(), "db-password/abc123", "x")
		require.ErrorContains(t, err, "does not accept a version")
		require.Zero(t, fake.setCalls)
	})

	t.Run("invalid key rejected before any client call", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		p := &azureProvider{client: fake, cfg: &Config{}}

		err := p.Set(context.Background(), "https://evil.vault.azure.net/x", "x")
		require.Error(t, err)
		require.Zero(t, fake.setCalls)
	})

	t.Run("soft-delete 409 surfaces plainly via classify", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{
			setErr: newResponseError(http.StatusConflict, "Conflict", "ObjectIsDeletedButRecoverable", ""),
		}
		p := &azureProvider{client: fake, cfg: &Config{}}

		err := p.Set(context.Background(), "db-password", "x")
		require.ErrorContains(t, err, "soft-deleted")
		requireNotTyped(t, err)
	})

	t.Run("transient backend error -> StoreUnavailableError", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{
			setErr: newResponseError(http.StatusInternalServerError, "InternalError", "", ""),
		}
		p := &azureProvider{client: fake, cfg: &Config{}}

		requireStoreUnavailable(t, p.Set(context.Background(), "db-password", "x"))
	})

	t.Run("self-bounds its deadline on a deadline-less context", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{blockSet: true}
		p := &azureProvider{client: fake, cfg: &Config{}, timeout: 20 * time.Millisecond}

		err := p.Set(context.Background(), "db-password", "x")
		requireStoreUnavailable(t, err)
	})
}

func requireNotTyped(t *testing.T, err error) {
	t.Helper()

	var (
		knf *kv.KeyNotFoundError
		sue *kv.StoreUnavailableError
	)

	require.Error(t, err)
	require.False(t, errors.As(err, &knf), "must not be *kv.KeyNotFoundError")
	require.False(t, errors.As(err, &sue), "must not be *kv.StoreUnavailableError")
}

func requireStoreUnavailable(t *testing.T, err error) {
	t.Helper()

	var sue *kv.StoreUnavailableError
	require.ErrorAs(t, err, &sue)
}

func TestCredential(t *testing.T) {
	t.Parallel()

	const (
		tenant = "11111111-1111-1111-1111-111111111111"
		client = "22222222-2222-2222-2222-222222222222"
	)

	tests := []struct {
		name string
		cfg  *Config
		want any
	}{
		{
			name: "managed_identity system-assigned",
			cfg:  &Config{CredentialType: "managed_identity"},
			want: &azidentity.ManagedIdentityCredential{},
		},
		{
			name: "managed_identity user-assigned",
			cfg:  &Config{CredentialType: "managed_identity", ClientID: client},
			want: &azidentity.ManagedIdentityCredential{},
		},
		{
			name: "workload_identity",
			cfg: &Config{
				CredentialType:     "workload_identity",
				TenantID:           tenant,
				ClientID:           client,
				FederatedTokenFile: "/var/run/secrets/azure/tokens/token",
			},
			want: &azidentity.WorkloadIdentityCredential{},
		},
		{
			name: "client_secret",
			cfg: &Config{
				CredentialType: "client_secret",
				TenantID:       tenant,
				ClientID:       client,
				ClientSecret:   "s3cr3t",
			},
			want: &azidentity.ClientSecretCredential{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cred, err := tt.cfg.credential()
			require.NoError(t, err)
			require.IsType(t, tt.want, cred)
		})
	}
}
