package gcp

import (
	"context"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestProvider(projectID, location string) *gcpProvider {
	return &gcpProvider{cfg: &Config{ProjectID: projectID, Location: location}}
}

func TestVersionName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		projectID string
		location  string
		key       string
		want      string
	}{
		// --- global (no location) ---
		{
			name:      "bare key gets latest",
			projectID: "my-proj",
			key:       "db-password",
			want:      "projects/my-proj/secrets/db-password/versions/latest",
		},
		{
			name:      "explicit version is preserved (no latest appended)",
			projectID: "my-proj",
			key:       "db-password/versions/7",
			want:      "projects/my-proj/secrets/db-password/versions/7",
		},
		{
			// A full-resource-name key must NOT escape the configured project: it is
			// nested under project_id, never used verbatim.
			name:      "full resource name does not escape the configured project",
			projectID: "my-proj",
			key:       "projects/other/secrets/x",
			want:      "projects/my-proj/secrets/projects/other/secrets/x/versions/latest",
		},

		// --- regional (location set) ---
		{
			name:      "regional bare key gets latest under /locations/",
			projectID: "my-proj",
			location:  "europe-west1",
			key:       "db-password",
			want:      "projects/my-proj/locations/europe-west1/secrets/db-password/versions/latest",
		},
		{
			name:      "regional explicit version is preserved",
			projectID: "my-proj",
			location:  "europe-west1",
			key:       "db-password/versions/2",
			want:      "projects/my-proj/locations/europe-west1/secrets/db-password/versions/2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newTestProvider(tt.projectID, tt.location)
			require.Equal(t, tt.want, p.versionName(tt.key))
		})
	}
}

func TestSecretName(t *testing.T) {
	t.Parallel()

	t.Run("global", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider("my-proj", "")
		require.Equal(t, "projects/my-proj/secrets/db-password", p.secretName("db-password"))
	})

	t.Run("regional", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider("my-proj", "europe-west1")
		require.Equal(t,
			"projects/my-proj/locations/europe-west1/secrets/db-password",
			p.secretName("db-password"),
		)
	})
}

func TestProjectParent(t *testing.T) {
	t.Parallel()

	t.Run("global", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "projects/my-proj", newTestProvider("my-proj", "").projectParent())
	})

	t.Run("regional", func(t *testing.T) {
		t.Parallel()

		require.Equal(t,
			"projects/my-proj/locations/europe-west1",
			newTestProvider("my-proj", "europe-west1").projectParent(),
		)
	})
}

func TestBuildAuthOptions_ImpersonationRejectsBadBase(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{cfg: &Config{
		ProjectID:                 "proj",
		ImpersonateServiceAccount: "target@proj.iam.gserviceaccount.com",
		CredentialsType:           "service_account",
		CredentialsJSON:           `{"type":"service_account"}`,
	}}

	_, err := p.buildAuthOptions(t.Context())
	require.Error(t, err)
}

func TestInit_WiresUsableClientFromSeam(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.seed("projects/proj/secrets/db-password", []byte("s3cr3t"))

	p := &gcpProvider{
		cfg:      &Config{ProjectID: "proj"},
		testOpts: startFakeServer(t, fake),
	}

	require.NoError(t, p.Init(t.Context()))
	require.NotNil(t, p.client, "Init must build the client")
	t.Cleanup(func() { _ = p.Close(t.Context()) })

	resp, err := p.client.AccessSecretVersion(t.Context(), &pb.AccessSecretVersionRequest{
		Name: "projects/proj/secrets/db-password/versions/latest",
	})
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", string(resp.GetPayload().GetData()))
}

func TestInit_SeamShortCircuitsAuthAssembly(t *testing.T) {
	t.Parallel()

	// Impersonation config is present, but testOpts must short-circuit the auth
	// switch entirely. If Init tried to build impersonated credentials here it
	// would reach out for ADC/tokens and make this test flaky or failing — the
	// seam winning is the contract being pinned.
	p := &gcpProvider{
		cfg: &Config{
			ProjectID:                 "proj",
			ImpersonateServiceAccount: "target@proj.iam.gserviceaccount.com",
		},
		testOpts: startFakeServer(t, newFakeSecretManager()),
	}

	require.NoError(t, p.Init(t.Context()))
	require.NotNil(t, p.client, "testOpts must win over the auth assembly")
	t.Cleanup(func() { _ = p.Close(t.Context()) })
}

func TestClose_NilClientIsSafe(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{cfg: &Config{ProjectID: "proj"}}
	require.NoError(t, p.Close(t.Context()))
}

func TestClose_ClosesInitializedClient(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{cfg: &Config{ProjectID: "proj"}, testOpts: startFakeServer(t, newFakeSecretManager())}
	require.NoError(t, p.Init(t.Context()))
	require.NoError(t, p.Close(t.Context()))
}

func TestEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		location  string
		transport string
		want      string
	}{
		// --- global: no endpoint override regardless of transport ---
		{
			name: "global default transport has no endpoint override",
			want: "",
		},
		{
			name:      "global grpc has no endpoint override",
			transport: "grpc",
			want:      "",
		},
		{
			name:      "global rest has no endpoint override",
			transport: "rest",
			want:      "",
		},

		// --- regional: format follows the transport ---
		{
			name:     "regional default transport uses the grpc host:443 endpoint",
			location: "europe-west1",
			want:     "secretmanager.europe-west1.rep.googleapis.com:443",
		},
		{
			name:      "regional grpc uses the grpc host:443 endpoint",
			location:  "europe-west1",
			transport: "grpc",
			want:      "secretmanager.europe-west1.rep.googleapis.com:443",
		},
		{
			name:      "regional rest uses the https URL endpoint",
			location:  "europe-west1",
			transport: "rest",
			want:      "https://secretmanager.europe-west1.rep.googleapis.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &gcpProvider{cfg: &Config{
				ProjectID: "proj",
				Location:  tt.location,
				Transport: tt.transport,
			}}
			require.Equal(t, tt.want, p.endpoint())
		})
	}
}

func TestInit_RESTTransportBuildsClient(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{
		cfg:      &Config{ProjectID: "proj", Transport: "rest"},
		testOpts: []option.ClientOption{option.WithoutAuthentication()},
	}

	require.NoError(t, p.Init(t.Context()))
	require.NotNil(t, p.client, "REST transport must build a client")
	t.Cleanup(func() { _ = p.Close(t.Context()) })
}

func newInitializedProvider(t *testing.T, cfg *Config, fake *fakeSecretManager) *gcpProvider {
	t.Helper()

	p := &gcpProvider{cfg: cfg, testOpts: startFakeServer(t, fake)}
	require.NoError(t, p.Init(t.Context()))
	t.Cleanup(func() { _ = p.Close(t.Context()) })

	return p
}

func TestGet_ReturnsPayloadVerbatim(t *testing.T) {
	t.Parallel()

	t.Run("returns the secret value", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/db-password", []byte("s3cr3t"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		got, err := p.Get(t.Context(), "db-password")
		require.NoError(t, err)
		require.Equal(t, "s3cr3t", got)
	})

	t.Run("empty payload is a successful empty value", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/blank", []byte(""))
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		got, err := p.Get(t.Context(), "blank")
		require.NoError(t, err)
		require.Equal(t, "", got)
	})

	t.Run("reads a pinned version", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/db", []byte("v1"), []byte("v2"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		got, err := p.Get(t.Context(), "db/versions/1")
		require.NoError(t, err)
		require.Equal(t, "v1", got)
	})

	t.Run("regional store resolves the /locations/-scoped name", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/locations/europe-west1/secrets/db-password", []byte("regional-secret"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj", Location: "europe-west1"}, fake)

		got, err := p.Get(t.Context(), "db-password")
		require.NoError(t, err)
		require.Equal(t, "regional-secret", got)
	})
}

func TestClassify(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{}

	const (
		kindNotFound    = "notfound"    // *kv.KeyNotFoundError
		kindUnavailable = "unavailable" // *kv.StoreUnavailableError
		kindPlain       = "plain"
	)

	tests := []struct {
		name string
		code codes.Code
		kind string
	}{
		{"not found", codes.NotFound, kindNotFound},
		{"unavailable", codes.Unavailable, kindUnavailable},
		{"deadline exceeded", codes.DeadlineExceeded, kindUnavailable},
		{"permission denied", codes.PermissionDenied, kindUnavailable},
		{"unauthenticated", codes.Unauthenticated, kindUnavailable},
		{"resource exhausted", codes.ResourceExhausted, kindUnavailable},
		{"failed precondition (disabled/destroyed version)", codes.FailedPrecondition, kindPlain},
		{"invalid argument (malformed name)", codes.InvalidArgument, kindPlain},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := p.classify("db", status.Error(tt.code, "boom"))
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

func TestGet_MapsRPCErrorThroughClassify(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.accessHook = func(context.Context, *pb.AccessSecretVersionRequest) (*pb.AccessSecretVersionResponse, error) {
		return nil, status.Error(codes.NotFound, "gone")
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	_, err := p.Get(t.Context(), "db")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound)
}

func TestGet_CRC32C(t *testing.T) {
	t.Parallel()

	t.Run("checksum mismatch is a store-unavailable error", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.accessHook = func(context.Context, *pb.AccessSecretVersionRequest) (*pb.AccessSecretVersionResponse, error) {
			wrong := int64(1) // deliberately wrong for the payload "value"

			return &pb.AccessSecretVersionResponse{
				Name:    "projects/proj/secrets/db/versions/latest",
				Payload: &pb.SecretPayload{Data: []byte("value"), DataCrc32C: &wrong},
			}, nil
		}
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		_, err := p.Get(t.Context(), "db")

		var unavailable *kv.StoreUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("absent checksum is accepted without panic", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.accessHook = func(context.Context, *pb.AccessSecretVersionRequest) (*pb.AccessSecretVersionResponse, error) {
			return &pb.AccessSecretVersionResponse{
				Payload: &pb.SecretPayload{Data: []byte("value"), DataCrc32C: nil},
			}, nil
		}
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		got, err := p.Get(t.Context(), "db")
		require.NoError(t, err)
		require.Equal(t, "value", got)
	})
}

func TestGet_TrimTrailingNewline(t *testing.T) {
	t.Parallel()

	t.Run("strips exactly one trailing newline after CRC verification", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/tok", []byte("value\n"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj", TrimTrailingNewline: true}, fake)

		got, err := p.Get(t.Context(), "tok")
		require.NoError(t, err)
		require.Equal(t, "value", got)
	})

	t.Run("strips only one newline", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/tok", []byte("value\n\n"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj", TrimTrailingNewline: true}, fake)

		got, err := p.Get(t.Context(), "tok")
		require.NoError(t, err)
		require.Equal(t, "value\n", got)
	})

	t.Run("preserves the newline when disabled", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSecretManager()
		fake.seed("projects/proj/secrets/tok", []byte("value\n"))
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		got, err := p.Get(t.Context(), "tok")
		require.NoError(t, err)
		require.Equal(t, "value\n", got)
	})
}

func TestGet_HonorsContextDeadline(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.accessHook = func(
		ctx context.Context, _ *pb.AccessSecretVersionRequest,
	) (*pb.AccessSecretVersionResponse, error) {
		<-ctx.Done() // block until the caller's deadline fires
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	_, err := p.Get(ctx, "db")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, codes.DeadlineExceeded, status.Code(unavailable.Err))
}

func TestGet_RejectsInvalidKeysBeforeRPC(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"", "projects/other/secrets/x", "a/b"} {
		t.Run("key="+key, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSecretManager()
			fake.accessHook = func(context.Context, *pb.AccessSecretVersionRequest) (*pb.AccessSecretVersionResponse, error) {
				t.Errorf("Get must reject key %q before making an RPC", key)
				return nil, status.Error(codes.Internal, "unexpected")
			}
			p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

			_, err := p.Get(t.Context(), key)
			require.Error(t, err)
		})
	}
}

func TestSet_AddsVersionToExistingSecret(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.seed("projects/proj/secrets/db", []byte("old"))
	fake.createHook = func(context.Context, *pb.CreateSecretRequest) (*pb.Secret, error) {
		t.Errorf("CreateSecret must not be called when the secret already exists")
		return nil, status.Error(codes.Internal, "unexpected")
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	require.NoError(t, p.Set(t.Context(), "db", "new"))

	got, err := p.Get(t.Context(), "db")
	require.NoError(t, err)
	require.Equal(t, "new", got)
}

func TestSet_SendsPayloadChecksum(t *testing.T) {
	t.Parallel()

	var gotCRC *int64
	fake := newFakeSecretManager()
	fake.addHook = func(_ context.Context, req *pb.AddSecretVersionRequest) (*pb.SecretVersion, error) {
		gotCRC = req.GetPayload().DataCrc32C
		return &pb.SecretVersion{Name: req.GetParent() + "/versions/1"}, nil
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	require.NoError(t, p.Set(t.Context(), "db", "newval"))
	require.NotNil(t, gotCRC, "Set must send a CRC32C on write")
	require.Equal(t, int64(crc32.Checksum([]byte("newval"), castagnoli)), *gotCRC)
}

func TestSet_CreatesSecretOnMissing(t *testing.T) {
	t.Parallel()

	// "db" does not exist: Set must AddSecretVersion(NotFound) → CreateSecret → retry.
	fake := newFakeSecretManager()
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	require.NoError(t, p.Set(t.Context(), "db", "created"))

	got, err := p.Get(t.Context(), "db")
	require.NoError(t, err)
	require.Equal(t, "created", got)
}

func TestSet_ToleratesConcurrentAlreadyExists(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.createHook = func(_ context.Context, req *pb.CreateSecretRequest) (*pb.Secret, error) {
		// Simulate a racing creator: the container now exists, so the retry add works.
		fake.seed(req.GetParent() + "/secrets/" + req.GetSecretId())
		return nil, status.Error(codes.AlreadyExists, "raced")
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	require.NoError(t, p.Set(t.Context(), "db", "val"))

	got, err := p.Get(t.Context(), "db")
	require.NoError(t, err)
	require.Equal(t, "val", got)
}

func TestSet_RejectsVersionPinnedKey(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.addHook = func(context.Context, *pb.AddSecretVersionRequest) (*pb.SecretVersion, error) {
		t.Errorf("Set must reject a version-pinned key before any RPC")
		return nil, status.Error(codes.Internal, "unexpected")
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	require.Error(t, p.Set(t.Context(), "db/versions/2", "val"))
}

func TestSet_RejectsInvalidKeysBeforeRPC(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"", "projects/other/secrets/x", "a/b"} {
		t.Run("key="+key, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSecretManager()
			fake.addHook = func(context.Context, *pb.AddSecretVersionRequest) (*pb.SecretVersion, error) {
				t.Errorf("Set must reject key %q before any RPC", key)
				return nil, status.Error(codes.Internal, "unexpected")
			}
			p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

			require.Error(t, p.Set(t.Context(), key, "val"))
		})
	}
}

func TestSet_BackendErrorIsClassified(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.seed("projects/proj/secrets/db")
	fake.addHook = func(context.Context, *pb.AddSecretVersionRequest) (*pb.SecretVersion, error) {
		return nil, status.Error(codes.PermissionDenied, "no access")
	}
	p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

	err := p.Set(t.Context(), "db", "val")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable)
}

func TestSet_CreateSecretShape(t *testing.T) {
	t.Parallel()

	captureCreate := func(fake *fakeSecretManager, out **pb.CreateSecretRequest) {
		fake.createHook = func(_ context.Context, req *pb.CreateSecretRequest) (*pb.Secret, error) {
			*out = req
			fake.seed(req.GetParent() + "/secrets/" + req.GetSecretId())

			return &pb.Secret{Name: req.GetParent() + "/secrets/" + req.GetSecretId()}, nil
		}
	}

	t.Run("global uses automatic replication", func(t *testing.T) {
		t.Parallel()

		var got *pb.CreateSecretRequest
		fake := newFakeSecretManager()
		captureCreate(fake, &got)
		p := newInitializedProvider(t, &Config{ProjectID: "proj"}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "val"))
		require.NotNil(t, got)
		require.Equal(t, "projects/proj", got.GetParent())
		require.Equal(t, "db", got.GetSecretId())
		require.NotNil(t, got.GetSecret().GetReplication().GetAutomatic(),
			"global secrets must use automatic replication")
	})

	t.Run("regional scopes under /locations/ and omits replication", func(t *testing.T) {
		t.Parallel()

		var got *pb.CreateSecretRequest
		fake := newFakeSecretManager()
		captureCreate(fake, &got)
		p := newInitializedProvider(t, &Config{ProjectID: "proj", Location: "europe-west1"}, fake)

		require.NoError(t, p.Set(t.Context(), "db", "val"))
		require.NotNil(t, got)
		require.Equal(t, "projects/proj/locations/europe-west1", got.GetParent())
		require.Equal(t, "db", got.GetSecretId())
		require.Nil(t, got.GetSecret().GetReplication(),
			"regional secrets must omit the replication policy")
	})
}

func TestSet_HonorsSelfTimeout(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.seed("projects/proj/secrets/db")
	fake.addHook = func(ctx context.Context, _ *pb.AddSecretVersionRequest) (*pb.SecretVersion, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	p := &gcpProvider{
		cfg:      &Config{ProjectID: "proj"},
		timeout:  30 * time.Millisecond,
		testOpts: startFakeServer(t, fake),
	}
	require.NoError(t, p.Init(t.Context()))
	t.Cleanup(func() { _ = p.Close(t.Context()) })

	err := p.Set(t.Context(), "db", "val")

	var unavailable *kv.StoreUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, codes.DeadlineExceeded, status.Code(unavailable.Err))
}

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
		{
			name: "valid aws imds source",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"credential_source":{"environment_id":"aws1",` +
				`"region_url":"http://169.254.169.254/latest/meta-data/placement/availability-zone",` +
				`"url":"http://169.254.169.254/latest/meta-data/iam/security-credentials",` +
				`"imdsv2_session_token_url":"http://169.254.169.254/latest/api/token"}}`,
		},
		{
			name: "valid aws imds source over ipv6",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"credential_source":{"environment_id":"aws1",` +
				`"url":"http://[fd00:ec2::254]/latest/meta-data/iam/security-credentials"}}`,
		},
		{
			name: "tampered aws credential source url",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"credential_source":{"environment_id":"aws1",` +
				`"url":"http://evil.example.com/latest/meta-data/iam/security-credentials"}}`,
			wantErr:     true,
			errContains: "IMDS",
		},
		{
			name: "non-aws url source is not imds-restricted",
			json: `{"type":"external_account",` +
				`"token_url":"https://sts.googleapis.com/v1/token",` +
				`"credential_source":{"url":"https://my-idp.example.com/token"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{
				CredentialsType: "external_account",
				CredentialsJSON: tt.json,
			}
			err := cfg.validateExternalAccount()

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

		cfg := &Config{
			CredentialsType: "external_account",
			CredentialsFile: path,
		}
		require.NoError(t, cfg.validateExternalAccount())
	})

	t.Run("unreadable credentials_file is an error", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			CredentialsType: "external_account",
			CredentialsFile: filepath.Join(t.TempDir(), "missing.json"),
		}
		err := cfg.validateExternalAccount()
		require.Error(t, err)
	})
}
