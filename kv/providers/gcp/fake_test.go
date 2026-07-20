package gcp

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	pb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSecretManager struct {
	pb.UnimplementedSecretManagerServiceServer

	mu sync.Mutex
	// secrets is keyed by the secret resource name (up to and including
	// "/secrets/<id>"); the value is that secret's ordered version payloads,
	// 1-indexed by position.
	secrets map[string][][]byte

	// Optional per-RPC overrides. When non-nil, the hook replaces the default.
	accessHook func(context.Context, *pb.AccessSecretVersionRequest) (*pb.AccessSecretVersionResponse, error)
	addHook    func(context.Context, *pb.AddSecretVersionRequest) (*pb.SecretVersion, error)
	createHook func(context.Context, *pb.CreateSecretRequest) (*pb.Secret, error)
}

func newFakeSecretManager() *fakeSecretManager {
	return &fakeSecretManager{secrets: make(map[string][][]byte)}
}

// seed appends one or more versions to a secret, creating it if absent.
func (f *fakeSecretManager) seed(secretName string, versions ...[]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.secrets[secretName] = append(f.secrets[secretName], versions...)
}

func (f *fakeSecretManager) AccessSecretVersion(
	ctx context.Context,
	req *pb.AccessSecretVersionRequest,
) (*pb.AccessSecretVersionResponse, error) {
	if f.accessHook != nil {
		return f.accessHook(ctx, req)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	secretName, version, ok := strings.Cut(req.GetName(), "/versions/")
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "malformed name %q", req.GetName())
	}

	versions, ok := f.secrets[secretName]
	if !ok || len(versions) == 0 {
		return nil, status.Errorf(codes.NotFound, "secret %q not found", secretName)
	}

	data := versions[len(versions)-1] // "latest"

	if version != "latest" {
		n, err := strconv.Atoi(version)
		if err != nil || n < 1 || n > len(versions) {
			return nil, status.Errorf(codes.NotFound, "version %q not found", version)
		}

		data = versions[n-1]
	}

	crc := int64(crc32.Checksum(data, castagnoli))

	return &pb.AccessSecretVersionResponse{
		Name:    req.GetName(),
		Payload: &pb.SecretPayload{Data: data, DataCrc32C: &crc},
	}, nil
}

func (f *fakeSecretManager) AddSecretVersion(
	ctx context.Context,
	req *pb.AddSecretVersionRequest,
) (*pb.SecretVersion, error) {
	if f.addHook != nil {
		return f.addHook(ctx, req)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	parent := req.GetParent()
	if _, ok := f.secrets[parent]; !ok {
		return nil, status.Errorf(codes.NotFound, "secret %q not found", parent)
	}

	f.secrets[parent] = append(f.secrets[parent], req.GetPayload().GetData())

	return &pb.SecretVersion{
		Name: fmt.Sprintf("%s/versions/%d", parent, len(f.secrets[parent])),
	}, nil
}

func (f *fakeSecretManager) CreateSecret(
	ctx context.Context,
	req *pb.CreateSecretRequest,
) (*pb.Secret, error) {
	if f.createHook != nil {
		return f.createHook(ctx, req)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	name := req.GetParent() + "/secrets/" + req.GetSecretId()
	if _, exists := f.secrets[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "secret %q already exists", name)
	}

	f.secrets[name] = [][]byte{}

	return &pb.Secret{Name: name}, nil
}

func startFakeServer(t *testing.T, fake *fakeSecretManager) []option.ClientOption {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)

	srv := grpc.NewServer()
	pb.RegisterSecretManagerServiceServer(srv, fake)

	go func() {
		//nolint:errcheck
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return []option.ClientOption{option.WithGRPCConn(conn)}
}

func TestFakeServer_DialAndRead(t *testing.T) {
	t.Parallel()

	fake := newFakeSecretManager()
	fake.seed("projects/p/secrets/db-password", []byte("s3cr3t"))

	opts := startFakeServer(t, fake)

	client, err := secretmanager.NewClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	resp, err := client.AccessSecretVersion(t.Context(), &pb.AccessSecretVersionRequest{
		Name: "projects/p/secrets/db-password/versions/latest",
	})
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", string(resp.GetPayload().GetData()))
	require.NotNil(t, resp.GetPayload().DataCrc32C, "fake must return a CRC")
}
