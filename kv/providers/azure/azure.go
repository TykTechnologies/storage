package azure

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
)

type secretsClient interface {
	GetSecret(
		ctx context.Context,
		name,
		version string,
		o *azsecrets.GetSecretOptions,
	) (azsecrets.GetSecretResponse, error)
	SetSecret(
		ctx context.Context,
		name string,
		p azsecrets.SetSecretParameters,
		o *azsecrets.SetSecretOptions,
	) (azsecrets.SetSecretResponse, error)
}

type azureProvider struct {
	cfg     *Config
	client  secretsClient
	timeout time.Duration
}

func (ap *azureProvider) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (ap *azureProvider) Set(ctx context.Context, key, value string) error {
	return nil
}

func (ap *azureProvider) Timeout() time.Duration {
	return ap.timeout
}
