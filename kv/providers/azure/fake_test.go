package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/require"
)

type fakeSecretsClient struct {
	getResp azsecrets.GetSecretResponse
	getErr  error
	setResp azsecrets.SetSecretResponse
	setErr  error

	// blockSet makes SetSecret block until the context is cancelled, then return ctx.Err()
	blockSet bool

	gotGetName    string
	gotGetVersion string
	gotSetName    string
	gotSetValue   string
	setCalls      int
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
	ctx context.Context, name string, p azsecrets.SetSecretParameters, _ *azsecrets.SetSecretOptions,
) (azsecrets.SetSecretResponse, error) {
	f.setCalls++
	f.gotSetName = name

	if p.Value != nil {
		f.gotSetValue = *p.Value
	}

	if f.blockSet {
		<-ctx.Done()

		return azsecrets.SetSecretResponse{}, ctx.Err()
	}

	return f.setResp, f.setErr
}

func getSecretResponse(value string) azsecrets.GetSecretResponse {
	return azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &value}}
}

// newResponseError builds an *azcore.ResponseError shaped like a real Key Vault error: the
// given HTTP status, the outer code echoed in the x-ms-error-code header, a body carrying
// error.code + error.innererror.code, and (when non-empty) an x-ms-request-id header.
func newResponseError(statusCode int, errorCode, innerCode, requestID string) *azcore.ResponseError {
	body := fmt.Sprintf(
		`{"error":{"code":%q,"message":"test error","innererror":{"code":%q}}}`, errorCode, innerCode)

	header := http.Header{}
	header.Set("x-ms-error-code", errorCode)

	if requestID != "" {
		header.Set("x-ms-request-id", requestID)
	}

	return &azcore.ResponseError{
		StatusCode: statusCode,
		ErrorCode:  errorCode,
		RawResponse: &http.Response{
			StatusCode: statusCode,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader(body)),
		},
	}
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
