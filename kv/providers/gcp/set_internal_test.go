package gcp

import (
	"context"
	"hash/crc32"
	"testing"
	"time"

	pb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
