package azure

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

var (
	// secretNameRe is Azure's secret-name grammar: 1+ of [0-9a-zA-Z-]. A name that is a
	// full URL or carries any other character (":", ".", "/", "_", "..") fails to match,
	// so a crafted key cannot inject into the vault URL path.
	secretNameRe = regexp.MustCompile(`^[0-9a-zA-Z-]+$`)

	// secretVersionRe matches an Azure-assigned version identifier (opaque; real values are
	// 32 hex chars). Alphanumeric is a safe superset — we do not parse it as hex.
	secretVersionRe = regexp.MustCompile(`^[0-9a-zA-Z]+$`)
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

// validateSecretKey splits the reference Path into a bare secret name and an optional
// version, rejecting anything that is not a bare name (optionally suffixed "/<version>").
func validateSecretKey(key string) (name, version string, err error) {
	name, version, _ = strings.Cut(key, "/")

	switch {
	case name == "":
		return "", "", fmt.Errorf("azure: empty secret key")
	case !secretNameRe.MatchString(name):
		return "", "", fmt.Errorf(
			"azure: secret key %q must be a bare secret name ([A-Za-z0-9-]); full URLs and paths are not allowed", key)
	case strings.Contains(version, "/"):
		return "", "", fmt.Errorf(
			"azure: secret key %q has too many path segments (want name or name/version)", key)
	case version != "" && !secretVersionRe.MatchString(version):
		return "", "", fmt.Errorf("azure: secret version %q is not a valid version identifier", version)
	}

	return name, version, nil
}
