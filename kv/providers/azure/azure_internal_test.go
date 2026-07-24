package azure

import (
	"testing"

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
