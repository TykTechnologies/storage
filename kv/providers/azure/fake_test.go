package azure

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/require"
)

type fakeSecretsClient struct {
	getResp azsecrets.GetSecretResponse
	getErr  error
	setResp azsecrets.SetSecretResponse
	setErr  error

	gotGetName    string
	gotGetVersion string
	gotSetName    string
	gotSetValue   string
}

var _ secretsClient = (*fakeSecretsClient)(nil)

func (f *fakeSecretsClient) GetSecret(
	_ context.Context, name, version string, _ *azsecrets.GetSecretOptions,
) (azsecrets.GetSecretResponse, error) {
	f.gotGetName = name
	f.gotGetVersion = version

	return f.getResp, f.getErr
}

func (f *fakeSecretsClient) SetSecret(
	_ context.Context, name string, p azsecrets.SetSecretParameters, _ *azsecrets.SetSecretOptions,
) (azsecrets.SetSecretResponse, error) {
	f.gotSetName = name
	if p.Value != nil {
		f.gotSetValue = *p.Value
	}

	return f.setResp, f.setErr
}

func getSecretResponse(value string) azsecrets.GetSecretResponse {
	return azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &value}}
}

func TestFakeSecretsClient_Seam(t *testing.T) {
	t.Parallel()

	t.Run("provider accepts fake; get returns canned response", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{getResp: getSecretResponse("s3cr3t")}
		p := &azureProvider{client: fake}

		resp, err := p.client.GetSecret(context.Background(), "db-password", "v1", nil)
		require.NoError(t, err)
		require.NotNil(t, resp.Value)
		require.Equal(t, "s3cr3t", *resp.Value)
		require.Equal(t, "db-password", fake.gotGetName)
		require.Equal(t, "v1", fake.gotGetVersion)
	})

	t.Run("get propagates a supplied error", func(t *testing.T) {
		t.Parallel()

		sentinel := errors.New("boom")
		fake := &fakeSecretsClient{getErr: sentinel}

		_, err := fake.GetSecret(context.Background(), "k", "", nil)
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("set records the value written", func(t *testing.T) {
		t.Parallel()

		fake := &fakeSecretsClient{}
		value := "new-value"

		_, err := fake.SetSecret(context.Background(), "db-password",
			azsecrets.SetSecretParameters{Value: &value}, nil)
		require.NoError(t, err)
		require.Equal(t, "db-password", fake.gotSetName)
		require.Equal(t, "new-value", fake.gotSetValue)
	})
}
