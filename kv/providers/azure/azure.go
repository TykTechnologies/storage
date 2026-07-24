package azure

import (
	"context"
	"time"
)

type azureProvider struct {
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
