package aws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T, cfg *Config, fake *fakeSecretsManager) *awsProvider {
	t.Helper()

	if cfg.Region == "" {
		cfg.Region = "eu-central-1"
	}

	p := &awsProvider{cfg: cfg, testClient: fake}
	require.NoError(t, p.Init(t.Context()))

	return p
}

func TestGet_ReturnsPayloadVerbatim(t *testing.T) {
	t.Parallel()

	t.Run("returns SecretString", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("db-password", strVersion("v1", "s3cr3t"))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), "db-password")
		require.NoError(t, err)
		require.Equal(t, "s3cr3t", got)
	})

	t.Run("returns SecretBinary as string", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("blob", binVersion("v1", []byte{0x74, 0x79, 0x6b}))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), "blob")
		require.NoError(t, err)
		require.Equal(t, "tyk", got)
	})

	t.Run("empty string payload is a successful empty value", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("blank", strVersion("v1", ""))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), "blank")
		require.NoError(t, err)
		require.Equal(t, "", got)
	})

	t.Run("payload with neither string nor binary is store-unavailable", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("corrupt", fakeVersion{id: "v1"})
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(), "corrupt")

		var unavailable *kv.StoreUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("slash-path names pass through verbatim", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("prod/db/password", strVersion("v1", "deep"))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), "prod/db/password")
		require.NoError(t, err)
		require.Equal(t, "deep", got)
	})

	t.Run("full ARN in the store region passes through verbatim", func(t *testing.T) {
		t.Parallel()

		const arn = "arn:aws:secretsmanager:eu-central-1:123456789012:secret:db-password-AbCdEf"

		fake := newFakeSecretsManager()
		fake.seed(arn, strVersion("v1", "by-arn"))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), arn)
		require.NoError(t, err)
		require.Equal(t, "by-arn", got)
	})
}

func TestGet_VersionSelection(t *testing.T) {
	t.Parallel()

	seedTwo := func() *fakeSecretsManager {
		fake := newFakeSecretsManager()
		fake.seed("db", strVersion("v1", "old"), strVersion("v2", "new"))

		return fake
	}

	t.Run("default reads AWSCURRENT", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider(t, &Config{}, seedTwo())

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "new", got)
	})

	t.Run("version_stage AWSPREVIOUS reads the prior version", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider(t, &Config{VersionStage: "AWSPREVIOUS"}, seedTwo())

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "old", got)
	})

	t.Run("version_id reads the exact version", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider(t, &Config{VersionID: "v1"}, seedTwo())

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "old", got)
	})
}

func TestGet_TrimTrailingNewline(t *testing.T) {
	t.Parallel()

	t.Run("trims one trailing newline when enabled", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("nl", strVersion("v1", "value\n\n"))
		p := newTestProvider(t, &Config{TrimTrailingNewline: true}, fake)

		got, err := p.Get(t.Context(), "nl")
		require.NoError(t, err)
		require.Equal(t, "value\n", got)
	})

	t.Run("keeps the newline when disabled", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("nl", strVersion("v1", "value\n"))
		p := newTestProvider(t, &Config{}, fake)

		got, err := p.Get(t.Context(), "nl")
		require.NoError(t, err)
		require.Equal(t, "value\n", got)
	})
}

func TestValidateSecretKey(t *testing.T) {
	t.Parallel()

	t.Run("empty key is rejected before any API call", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(), "")
		require.ErrorContains(t, err, "empty secret key")

		gets, _, _ := fake.calls()
		require.Zero(t, gets)
	})

	t.Run("ARN with mismatched region is rejected before any API call", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{Region: "eu-central-1"}, fake)

		_, err := p.Get(t.Context(),
			"arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf")
		require.ErrorContains(t, err, "does not match")
		require.ErrorContains(t, err, "us-east-1")

		gets, _, _ := fake.calls()
		require.Zero(t, gets)
	})

	t.Run("ARN of another service is rejected", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(),
			"arn:aws:s3:eu-central-1:123456789012:secret:db")
		require.ErrorContains(t, err, "secretsmanager")

		gets, _, _ := fake.calls()
		require.Zero(t, gets)
	})

	t.Run("malformed ARN is rejected", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(), "arn:aws:secretsmanager")
		require.ErrorContains(t, err, "invalid")

		gets, _, _ := fake.calls()
		require.Zero(t, gets)
	})
}

func TestClassify(t *testing.T) {
	t.Parallel()

	p := &awsProvider{}

	const (
		kindNotFound    = "notfound"    // *kv.KeyNotFoundError
		kindUnavailable = "unavailable" // *kv.StoreUnavailableError
		kindPlain       = "plain"
	)

	tests := []struct {
		name string
		err  error
		kind string
	}{
		{"resource not found", &types.ResourceNotFoundException{}, kindNotFound},
		{"decryption failure", &types.DecryptionFailure{}, kindUnavailable},
		{"internal service error", &types.InternalServiceError{}, kindUnavailable},
		{"opaque transport error", errors.New("dial tcp: connection refused"), kindUnavailable},
		{"context deadline", context.DeadlineExceeded, kindUnavailable},
		{"invalid parameter", &types.InvalidParameterException{}, kindPlain},
		{"invalid request (scheduled for deletion)", &types.InvalidRequestException{}, kindPlain},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := p.classify("db", tt.err)
			require.Error(t, err)

			var notFound *kv.KeyNotFoundError
			var unavailable *kv.StoreUnavailableError

			switch tt.kind {
			case kindNotFound:
				require.ErrorAs(t, err, &notFound)
			case kindUnavailable:
				require.ErrorAs(t, err, &unavailable)
			case kindPlain:
				require.False(t, errors.As(err, &notFound))
				require.False(t, errors.As(err, &unavailable))
			}
		})
	}
}

func TestGet_MapsAPIErrorThroughClassify(t *testing.T) {
	t.Parallel()

	t.Run("missing secret is a key-not-found error", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(), "nope")

		var notFound *kv.KeyNotFoundError
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("hook failure surfaces as store-unavailable", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.getHook = func(context.Context, *secretsmanager.GetSecretValueInput) (
			*secretsmanager.GetSecretValueOutput, error,
		) {
			return nil, &types.InternalServiceError{}
		}
		p := newTestProvider(t, &Config{}, fake)

		_, err := p.Get(t.Context(), "db")

		var unavailable *kv.StoreUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("caller context deadline surfaces as store-unavailable", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.getHook = func(ctx context.Context, _ *secretsmanager.GetSecretValueInput) (
			*secretsmanager.GetSecretValueOutput, error,
		) {
			<-ctx.Done()

			return nil, ctx.Err()
		}
		p := newTestProvider(t, &Config{}, fake)

		ctx, cancel := context.WithTimeout(t.Context(), 0)
		defer cancel()

		_, err := p.Get(ctx, "db")

		var unavailable *kv.StoreUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})
}

func TestSet_WritesNewVersion(t *testing.T) {
	t.Parallel()

	t.Run("adds a version to an existing secret", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("db", strVersion("v1", "old"))
		p := newTestProvider(t, &Config{}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "new"))

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "new", got)

		_, puts, creates := fake.calls()
		require.Equal(t, 1, puts)
		require.Zero(t, creates)
	})

	t.Run("creates the secret when it does not exist", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		require.NoError(t, p.Set(t.Context(), "fresh", "value"))

		got, err := p.Get(t.Context(), "fresh")
		require.NoError(t, err)
		require.Equal(t, "value", got)

		_, _, creates := fake.calls()
		require.Equal(t, 1, creates)
	})

	t.Run("tolerates a concurrent create and retries the put", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.createHook = func(context.Context, *secretsmanager.CreateSecretInput) (
			*secretsmanager.CreateSecretOutput, error,
		) {
			// Simulate another writer winning the create race.
			fake.seed("db", strVersion("v1", "other-writer"))

			return nil, &types.ResourceExistsException{}
		}
		p := newTestProvider(t, &Config{}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "mine"))

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "mine", got)

		_, puts, creates := fake.calls()
		require.Equal(t, 2, puts)
		require.Equal(t, 1, creates)
	})
}

func TestSet_Rejections(t *testing.T) {
	t.Parallel()

	t.Run("rejected on a version_id-pinned store, without any API call", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{VersionID: "v1"}, fake)

		err := p.Set(t.Context(), "db", "x")
		require.ErrorContains(t, err, "version-pinned")

		_, puts, creates := fake.calls()
		require.Zero(t, puts)
		require.Zero(t, creates)
	})

	t.Run("rejected on a non-AWSCURRENT version_stage", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{VersionStage: "AWSPREVIOUS"}, fake)

		err := p.Set(t.Context(), "db", "x")
		require.ErrorContains(t, err, "version-pinned")
	})

	t.Run("allowed on an explicit AWSCURRENT version_stage", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.seed("db", strVersion("v1", "old"))
		p := newTestProvider(t, &Config{VersionStage: "AWSCURRENT"}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "new"))
	})

	t.Run("empty key rejected without any API call", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		p := newTestProvider(t, &Config{}, fake)

		err := p.Set(t.Context(), "", "x")
		require.ErrorContains(t, err, "empty secret key")

		_, puts, creates := fake.calls()
		require.Zero(t, puts)
		require.Zero(t, creates)
	})
}

func TestSet_ErrorMapping(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretsManager()
	fake.putHook = func(context.Context, *secretsmanager.PutSecretValueInput) (
		*secretsmanager.PutSecretValueOutput, error,
	) {
		return nil, &types.InternalServiceError{}
	}
	p := newTestProvider(t, &Config{}, fake)

	err := p.Set(t.Context(), "db", "x")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable)
}

func TestSet_SelfManagedDeadline(t *testing.T) {
	t.Parallel()

	deadlineIn := func(t *testing.T, ctx context.Context) time.Duration {
		t.Helper()

		deadline, ok := ctx.Deadline()
		require.True(t, ok, "Set must run under a deadline even when the caller has none")

		return time.Until(deadline)
	}

	t.Run("applies the 5s default when the caller has no deadline", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.putHook = func(ctx context.Context, _ *secretsmanager.PutSecretValueInput) (
			*secretsmanager.PutSecretValueOutput, error,
		) {
			remaining := deadlineIn(t, ctx)
			require.Greater(t, remaining, 4*time.Second)
			require.LessOrEqual(t, remaining, 5*time.Second)

			return &secretsmanager.PutSecretValueOutput{}, nil
		}
		p := newTestProvider(t, &Config{}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "x"))

		_, puts, _ := fake.calls()
		require.Equal(t, 1, puts, "Set must reach the API")
	})

	t.Run("applies the configured timeout", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretsManager()
		fake.putHook = func(ctx context.Context, _ *secretsmanager.PutSecretValueInput) (
			*secretsmanager.PutSecretValueOutput, error,
		) {
			require.LessOrEqual(t, deadlineIn(t, ctx), 200*time.Millisecond)

			return &secretsmanager.PutSecretValueOutput{}, nil
		}

		p := &awsProvider{
			cfg:        &Config{Region: "eu-central-1"},
			timeout:    200 * time.Millisecond,
			testClient: fake,
		}
		require.NoError(t, p.Init(t.Context()))

		require.NoError(t, p.Set(t.Context(), "db", "x"))

		_, puts, _ := fake.calls()
		require.Equal(t, 1, puts, "Set must reach the API")
	})
}

// initRealClient runs Init without the test seam and returns the built SDK client.
func initRealClient(t *testing.T, cfg *Config) *secretsmanager.Client {
	t.Helper()

	p := &awsProvider{cfg: cfg}
	require.NoError(t, p.Init(t.Context()))
	require.NotNil(t, p.client)

	client, ok := p.client.(*secretsmanager.Client)
	require.True(t, ok, "Init must build a real secretsmanager.Client")

	return client
}

func TestInit_BuildsRealClient(t *testing.T) {
	t.Parallel()

	t.Run("static keys resolve without network", func(t *testing.T) {
		t.Parallel()

		client := initRealClient(t, &Config{
			Region:          "eu-central-1",
			AccessKeyID:     "AKIA123",
			SecretAccessKey: "shhh",
			SessionToken:    "tok",
		})

		opts := client.Options()
		require.Equal(t, "eu-central-1", opts.Region)

		creds, err := opts.Credentials.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "AKIA123", creds.AccessKeyID)
		require.Equal(t, "shhh", creds.SecretAccessKey)
		require.Equal(t, "tok", creds.SessionToken)
	})

	t.Run("custom endpoint is honored", func(t *testing.T) {
		t.Parallel()

		client := initRealClient(t, &Config{
			Region:          "eu-central-1",
			Endpoint:        "http://localhost:4566",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
		})

		require.Equal(t, "http://localhost:4566", sdkaws.ToString(client.Options().BaseEndpoint))
	})

	t.Run("assume-role wires an STS credentials cache without calling STS", func(t *testing.T) {
		t.Parallel()

		client := initRealClient(t, &Config{
			Region:          "eu-central-1",
			AccessKeyID:     "AKIA123",
			SecretAccessKey: "shhh",
			RoleARN:         "arn:aws:iam::123456789012:role/tyk-secret-reader",
			ExternalID:      "xid",
			RoleSessionName: "tyk-gw",
		})

		_, ok := client.Options().Credentials.(*sdkaws.CredentialsCache)
		require.True(t, ok, "assume-role credentials must be wrapped in a CredentialsCache")
	})

	t.Run("missing shared-config profile fails at Init", func(t *testing.T) {
		t.Parallel()

		p := &awsProvider{cfg: &Config{
			Region:  "eu-central-1",
			Profile: "tyk-test-profile-that-does-not-exist",
		}}

		err := p.Init(t.Context())
		require.Error(t, err)
		require.ErrorContains(t, err, "aws:")
	})
}
