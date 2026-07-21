package gcp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const validWIF = `{"type":"external_account",` +
	`"token_url":"https://sts.googleapis.com/v1/token",` +
	`"credential_source":{"file":"/var/run/token"}}`

func TestValidateExternalAccount(t *testing.T) {
	t.Parallel()

	const googleImpersonationURL = `"https://iamcredentials.googleapis.com/v1/projects/-/` +
		`serviceAccounts/x@y.iam.gserviceaccount.com:generateAccessToken"`

	tests := []struct {
		name        string
		json        string
		wantErr     bool
		errContains string
	}{
		{
			name: "valid file source",
			json: validWIF,
		},
		{
			name: "valid with Google impersonation url",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"service_account_impersonation_url":` + googleImpersonationURL + `,` +
				`"credential_source":{"file":"/t"}}`,
		},
		{
			name: "wrong type",
			json: `{"type":"service_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token","credential_source":{"file":"/t"}}`,
			wantErr:     true,
			errContains: "type",
		},
		{
			name: "non-Google token_url",
			json: `{"type":"external_account",` +
				`"token_url":"https://evil.example.com/token","credential_source":{"file":"/t"}}`,
			wantErr:     true,
			errContains: "token_url",
		},
		{
			name: "non-Google impersonation url",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"service_account_impersonation_url":"https://evil.example.com/x:generateAccessToken",` +
				`"credential_source":{"file":"/t"}}`,
			wantErr:     true,
			errContains: "impersonation",
		},
		{
			name: "executable credential source rejected",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"credential_source":{"executable":{"command":"/bin/evil"}}}`,
			wantErr:     true,
			errContains: "executable",
		},
		{
			name:        "malformed json",
			json:        `{not json`,
			wantErr:     true,
			errContains: "invalid external_account",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateExternalAccount(&Config{
				CredentialsType: "external_account",
				CredentialsJSON: tt.json,
			})

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errContains)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateExternalAccount_FileSource(t *testing.T) {
	t.Parallel()

	t.Run("reads and validates from credentials_file", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "wif.json")
		require.NoError(t, os.WriteFile(path, []byte(validWIF), 0o600))

		require.NoError(t, validateExternalAccount(&Config{
			CredentialsType: "external_account",
			CredentialsFile: path,
		}))
	})

	t.Run("unreadable credentials_file is an error", func(t *testing.T) {
		t.Parallel()

		err := validateExternalAccount(&Config{
			CredentialsType: "external_account",
			CredentialsFile: filepath.Join(t.TempDir(), "missing.json"),
		})
		require.Error(t, err)
	})
}
