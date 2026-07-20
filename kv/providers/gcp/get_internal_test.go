package gcp

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
