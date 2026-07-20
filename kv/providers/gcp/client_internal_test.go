package gcp

import (
	"testing"

	pb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/require"
)

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
