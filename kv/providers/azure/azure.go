package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/TykTechnologies/storage/kv"
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

// Get fetches the current enabled version (or a pinned version) of a secret and returns its
// value verbatim, optionally trimming one trailing newline
func (ap *azureProvider) Get(ctx context.Context, key string) (string, error) {
	name, version, err := validateSecretKey(key)
	if err != nil {
		return "", err
	}

	resp, err := ap.client.GetSecret(ctx, name, version, nil)
	if err != nil {
		return "", ap.classify(key, err)
	}

	if resp.Value == nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: errors.New("azure: nil secret value")}
	}

	out := *resp.Value
	if ap.cfg.TrimTrailingNewline {
		out = strings.TrimSuffix(out, "\n")
	}

	return out, nil
}

// Set writes value as a new version of the secret, creating the secret on first write:
// azsecrets.SetSecret is a single-call upsert, so there is no create-on-missing dance.
// A key carrying a "/<version>" is rejected — versions are server-assigned.
func (ap *azureProvider) Set(ctx context.Context, key, value string) error {
	name, version, err := validateSecretKey(key)
	if err != nil {
		return err
	}

	if version != "" {
		return fmt.Errorf("azure: set does not accept a version in the key %q", key)
	}

	ctx, cancel := context.WithTimeout(ctx, kv.EffectiveTimeout(ap.timeout))
	defer cancel()

	if _, err := ap.client.SetSecret(ctx, name, azsecrets.SetSecretParameters{Value: &value}, nil); err != nil {
		return ap.classify(key, err)
	}

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

// classify maps an error from a GetSecret/SetSecret call into the three-bucket model.
// It is the only place an *azcore.ResponseError / HTTP status is interpreted.
// A non-HTTP error (transport, DNS, context) has no ResponseError and is treated as transient.
//
//	404                                   -> *kv.KeyNotFoundError (negative-cached)
//	400 / 409 / 403+innererror SecretDisabled -> plain azure: error (not cached)
//	401 / other 403 / 429 / 5xx / non-HTTP    -> *kv.StoreUnavailableError (transient-cached)
func (ap *azureProvider) classify(key string, err error) error {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	annotated := annotateRequestID(respErr, err)

	switch respErr.StatusCode {
	case http.StatusNotFound:
		return &kv.KeyNotFoundError{KeyPath: key}
	case http.StatusBadRequest:
		return fmt.Errorf("azure: invalid secret request for %q: %w", key, annotated)
	case http.StatusConflict:
		return fmt.Errorf("azure: secret %q is soft-deleted; recover or purge it: %w", key, annotated)
	case http.StatusForbidden:
		if innerErrorCode(respErr) == "SecretDisabled" {
			return fmt.Errorf("azure: secret disabled for %q: %w", key, annotated)
		}

		return &kv.StoreUnavailableError{KeyPath: key, Err: annotated}
	default:
		return &kv.StoreUnavailableError{KeyPath: key, Err: annotated}
	}
}

// annotateRequestID wraps err with the service's x-ms-request-id when present, so the
// correlation ID Microsoft support and the vault's AuditEvent logs key off lands in the
// gateway log.
func annotateRequestID(respErr *azcore.ResponseError, err error) error {
	if respErr.RawResponse == nil {
		return err
	}

	id := respErr.RawResponse.Header.Get("x-ms-request-id")
	if id == "" {
		return err
	}

	return fmt.Errorf("x-ms-request-id %s: %w", id, err)
}

// innerErrorCode extracts error.innererror.code from a Key Vault error body. It is the
// stable machine signal that distinguishes a disabled secret (SecretDisabled) from an
// access denial — both surface as 403 Forbidden.
func innerErrorCode(respErr *azcore.ResponseError) string {
	if respErr.RawResponse == nil || respErr.RawResponse.Body == nil {
		return ""
	}

	body, err := io.ReadAll(respErr.RawResponse.Body)
	if err != nil {
		return ""
	}

	var parsed struct {
		Error struct {
			InnerError struct {
				Code string `json:"code"`
			} `json:"innererror"`
		} `json:"error"`
	}

	if json.Unmarshal(body, &parsed) != nil {
		return ""
	}

	return parsed.Error.InnerError.Code
}
