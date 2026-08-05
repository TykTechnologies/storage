package gcp_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/gcp"
	"github.com/stretchr/testify/require"
)

const validExternalAccountJSON = `{` +
	`"type":"external_account",` +
	`"audience":"//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aws",` +
	`"subject_token_type":"urn:ietf:params:aws:token-type:aws4_request",` +
	`"token_url":"https://sts.googleapis.com/v1/token",` +
	`"credential_source":{"environment_id":"aws1",` +
	`"region_url":"http://169.254.169.254/latest/meta-data/placement/availability-zone",` +
	`"url":"http://169.254.169.254/latest/meta-data/iam/security-credentials"}` +
	`}`

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
			config:      `{"project_id":`,
			wantErr:     true,
			errContains: "invalid config",
		},
		{
			name:        "missing project_id",
			config:      `{}`,
			wantErr:     true,
			errContains: "project_id",
		},
		{
			name: "both credentials_file and credentials_json",
			config: `{"project_id":"p","credentials_type":"service_account",` +
				`"credentials_file":"/f","credentials_json":"{}"}`,
			wantErr:     true,
			errContains: "mutually exclusive",
		},
		{
			name:        "credentials without credentials_type",
			config:      `{"project_id":"p","credentials_file":"/f"}`,
			wantErr:     true,
			errContains: "credentials_type is required",
		},
		{
			name:        "unsupported credentials_type",
			config:      `{"project_id":"p","credentials_file":"/f","credentials_type":"bogus"}`,
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "impersonated_service_account credentials_type is rejected",
			config:      `{"project_id":"p","credentials_file":"/f","credentials_type":"impersonated_service_account"}`,
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "credentials_type without any credentials",
			config:      `{"project_id":"p","credentials_type":"service_account"}`,
			wantErr:     true,
			errContains: "credentials_type set without",
		},
		{
			name:        "delegates without impersonation target",
			config:      `{"project_id":"p","impersonate_delegates":["d@x.iam.gserviceaccount.com"]}`,
			wantErr:     true,
			errContains: "impersonate_delegates",
		},
		{
			name:        "unparseable timeout",
			config:      `{"project_id":"p","timeout":"5 seconds"}`,
			wantErr:     true,
			errContains: "timeout",
		},
		{
			name:        "unsupported transport",
			config:      `{"project_id":"p","transport":"http2"}`,
			wantErr:     true,
			errContains: "transport",
		},

		// --- valid: factory returns a provider, no error ---
		{
			name:   "adc (project_id only)",
			config: `{"project_id":"p"}`,
		},
		{
			name:   "explicit credentials_file",
			config: `{"project_id":"p","credentials_file":"/f","credentials_type":"service_account"}`,
		},
		{
			name:   "explicit credentials_json",
			config: `{"project_id":"p","credentials_json":"{}","credentials_type":"service_account"}`,
		},
		{
			name:   "authorized_user credentials_type",
			config: `{"project_id":"p","credentials_file":"/f","credentials_type":"authorized_user"}`,
		},
		{
			name: "external_account credentials_type accepted",
			config: `{"project_id":"p","credentials_type":"external_account",` +
				`"credentials_json":` + strconv.Quote(validExternalAccountJSON) + `}`,
		},
		{
			name:   "impersonation with adc base",
			config: `{"project_id":"p","impersonate_service_account":"t@x.iam.gserviceaccount.com"}`,
		},
		{
			name: "impersonation with explicit base",
			config: `{"project_id":"p","credentials_file":"/f","credentials_type":"service_account",` +
				`"impersonate_service_account":"t@x.iam.gserviceaccount.com"}`,
		},
		{
			name: "impersonation with delegates",
			config: `{"project_id":"p","impersonate_service_account":"t@x.iam.gserviceaccount.com",` +
				`"impersonate_delegates":["d@x.iam.gserviceaccount.com"]}`,
		},
		{
			name:   "regional location",
			config: `{"project_id":"p","location":"europe-west1"}`,
		},
		{
			name:   "quota_project_id",
			config: `{"project_id":"p","quota_project_id":"billing-proj"}`,
		},
		{
			name:   "transport rest",
			config: `{"project_id":"p","transport":"rest"}`,
		},
		{
			name:   "transport grpc explicit",
			config: `{"project_id":"p","transport":"grpc"}`,
		},
		{
			name:   "trim_trailing_newline",
			config: `{"project_id":"p","trim_trailing_newline":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			factory := gcp.NewFactory()

			provider, err := factory(json.RawMessage(tt.config))

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

func TestNewFactory_TimeoutExposedViaTimeouter(t *testing.T) {
	t.Parallel()

	t.Run("parsed timeout is exposed", func(t *testing.T) {
		t.Parallel()

		provider, err := gcp.NewFactory()(json.RawMessage(`{"project_id":"p","timeout":"7s"}`))
		require.NoError(t, err)

		timeouter, ok := kv.AsTimeouter(provider)
		require.True(t, ok, "gcp provider must implement Timeouter")
		require.Equal(t, 7*time.Second, timeouter.Timeout())
	})

	t.Run("negative timeout is zero (store default applied)", func(t *testing.T) {
		t.Parallel()

		provider, err := gcp.NewFactory()(json.RawMessage(`{"project_id":"p","timeout":"-10s"}`))
		require.NoError(t, err)

		timeouter, ok := kv.AsTimeouter(provider)
		require.True(t, ok)
		require.Zero(t, timeouter.Timeout())
	})

	t.Run("absent timeout is zero (store default applies)", func(t *testing.T) {
		t.Parallel()

		provider, err := gcp.NewFactory()(json.RawMessage(`{"project_id":"p"}`))
		require.NoError(t, err)

		timeouter, ok := kv.AsTimeouter(provider)
		require.True(t, ok)
		require.Zero(t, timeouter.Timeout())
	})
}

func TestProvider_IsNotStandalone(t *testing.T) {
	t.Parallel()

	p, err := gcp.NewFactory()(json.RawMessage(`{"project_id":"proj"}`))
	require.NoError(t, err)

	_, ok := kv.AsStandaloner(p)
	require.False(t, ok, "must NOT implement Standalone (stays cache/singleflight-wrapped)")
}
