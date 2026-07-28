package aws_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/aws"
	"github.com/stretchr/testify/require"
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
			config:      `{"region":`,
			wantErr:     true,
			errContains: "invalid config",
		},
		{
			name:        "missing region",
			config:      `{}`,
			wantErr:     true,
			errContains: "region",
		},
		{
			name:        "access_key_id without secret_access_key",
			config:      `{"region":"eu-central-1","access_key_id":"AKIA123"}`,
			wantErr:     true,
			errContains: "set together",
		},
		{
			name:        "secret_access_key without access_key_id",
			config:      `{"region":"eu-central-1","secret_access_key":"shhh"}`,
			wantErr:     true,
			errContains: "set together",
		},
		{
			name:        "session_token without static keys",
			config:      `{"region":"eu-central-1","session_token":"tok"}`,
			wantErr:     true,
			errContains: "session_token",
		},
		{
			name: "profile with static keys",
			config: `{"region":"eu-central-1","profile":"dev",` +
				`"access_key_id":"AKIA123","secret_access_key":"shhh"}`,
			wantErr:     true,
			errContains: "mutually exclusive",
		},
		{
			name:        "external_id without role_arn",
			config:      `{"region":"eu-central-1","external_id":"xid"}`,
			wantErr:     true,
			errContains: "role_arn",
		},
		{
			name:        "role_session_name without role_arn",
			config:      `{"region":"eu-central-1","role_session_name":"tyk"}`,
			wantErr:     true,
			errContains: "role_arn",
		},
		{
			name: "version_stage with version_id",
			config: `{"region":"eu-central-1",` +
				`"version_stage":"AWSPREVIOUS","version_id":"11111111-2222-3333-4444-555555555555"}`,
			wantErr:     true,
			errContains: "mutually exclusive",
		},
		{
			name:        "unparseable timeout",
			config:      `{"region":"eu-central-1","timeout":"5 seconds"}`,
			wantErr:     true,
			errContains: "timeout",
		},
		{
			name:        "endpoint without scheme",
			config:      `{"region":"eu-central-1","endpoint":"localhost:4566"}`,
			wantErr:     true,
			errContains: "endpoint",
		},
		{
			name:        "endpoint with unsupported scheme",
			config:      `{"region":"eu-central-1","endpoint":"ftp://localhost:4566"}`,
			wantErr:     true,
			errContains: "endpoint",
		},

		// --- valid: factory returns a provider, no error ---
		{
			name:   "default credential chain (region only)",
			config: `{"region":"eu-central-1"}`,
		},
		{
			name: "static keys",
			config: `{"region":"eu-central-1",` +
				`"access_key_id":"AKIA123","secret_access_key":"shhh"}`,
		},
		{
			name: "static keys with session_token",
			config: `{"region":"eu-central-1",` +
				`"access_key_id":"AKIA123","secret_access_key":"shhh","session_token":"tok"}`,
		},
		{
			name:   "shared config profile",
			config: `{"region":"eu-central-1","profile":"dev"}`,
		},
		{
			name:   "assume role over the default chain",
			config: `{"region":"eu-central-1","role_arn":"arn:aws:iam::123456789012:role/tyk-secret-reader"}`,
		},
		{
			name: "assume role with external_id and session name",
			config: `{"region":"eu-central-1",` +
				`"role_arn":"arn:aws:iam::123456789012:role/tyk-secret-reader",` +
				`"external_id":"xid","role_session_name":"tyk-gw"}`,
		},
		{
			name: "assume role over static base credentials",
			config: `{"region":"eu-central-1",` +
				`"access_key_id":"AKIA123","secret_access_key":"shhh",` +
				`"role_arn":"arn:aws:iam::123456789012:role/tyk-secret-reader"}`,
		},
		{
			name:   "pinned version_stage",
			config: `{"region":"eu-central-1","version_stage":"AWSPREVIOUS"}`,
		},
		{
			name:   "pinned version_id",
			config: `{"region":"eu-central-1","version_id":"11111111-2222-3333-4444-555555555555"}`,
		},
		{
			name:   "custom endpoint",
			config: `{"region":"eu-central-1","endpoint":"http://localhost:4566"}`,
		},
		{
			name:   "trim_trailing_newline and timeout",
			config: `{"region":"eu-central-1","trim_trailing_newline":true,"timeout":"3s"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := aws.NewFactory()(json.RawMessage(tt.config))

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errContains)
				require.Contains(t, err.Error(), "aws:")
				require.Nil(t, p)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
}

func TestProvider_Capabilities(t *testing.T) {
	t.Parallel()

	p, err := aws.NewFactory()(json.RawMessage(`{"region":"eu-central-1","timeout":"7s"}`))
	require.NoError(t, err)

	t.Run("exposes the configured timeout", func(t *testing.T) {
		t.Parallel()

		timeouter, ok := kv.AsTimeouter(p)
		require.True(t, ok)
		require.Equal(t, 7*time.Second, timeouter.Timeout())
	})

	t.Run("is a setter", func(t *testing.T) {
		t.Parallel()

		_, ok := kv.AsSetter(p)
		require.True(t, ok)
	})

	t.Run("is an initializer", func(t *testing.T) {
		t.Parallel()

		_, ok := kv.AsInitializer(p)
		require.True(t, ok)
	})

	t.Run("is not standaloner, so the registry wraps it in the caching store", func(t *testing.T) {
		t.Parallel()

		_, ok := kv.AsStandaloner(p)
		require.False(t, ok)
	})

	t.Run("is not a closer: SDK v2 clients hold no closable resources", func(t *testing.T) {
		t.Parallel()

		_, ok := kv.AsCloser(p)
		require.False(t, ok)
	})

	t.Run("zero timeout when unset", func(t *testing.T) {
		t.Parallel()

		p2, err := aws.NewFactory()(json.RawMessage(`{"region":"eu-central-1"}`))
		require.NoError(t, err)

		timeouter, ok := kv.AsTimeouter(p2)
		require.True(t, ok)
		require.Equal(t, time.Duration(0), timeouter.Timeout())
	})
}
