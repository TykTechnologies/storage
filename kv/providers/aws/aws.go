package aws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/kv"
	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

var (
	_ kv.Setter      = (*awsProvider)(nil)
	_ kv.Initializer = (*awsProvider)(nil)
	_ kv.Timeouter   = (*awsProvider)(nil)
)

// secretsManagerAPI is the narrow slice of the Secrets Manager client the
// provider uses; it exists so tests can inject an in-process fake.
type secretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput,
		optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	PutSecretValue(ctx context.Context, params *secretsmanager.PutSecretValueInput,
		optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error)
	CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput,
		optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
}

// awsProvider is one configured store: a single Secrets Manager client (built
// in Init, reused across concurrent calls) plus its resolved config and
// timeout. testClient, when set, replaces the built client in tests.
type awsProvider struct {
	cfg        *Config
	client     secretsManagerAPI
	timeout    time.Duration
	testClient secretsManagerAPI
}

// Init builds the one client. Credential resolution stays lazy and Init runs no
// reachability check, so a transient outage at boot doesn't disable the store;
// misconfigured auth surfaces on the first call, classified as store-unavailable.
func (ap *awsProvider) Init(ctx context.Context) error {
	if ap.testClient != nil {
		ap.client = ap.testClient

		return nil
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(ap.cfg.Region),
	}

	if ap.cfg.AccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ap.cfg.AccessKeyID, ap.cfg.SecretAccessKey, ap.cfg.SessionToken)))
	}

	if ap.cfg.Profile != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(ap.cfg.Profile))
	}

	sdkCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("aws: load SDK config: %w", err)
	}

	if ap.cfg.RoleARN != "" {
		assume := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(sdkCfg), ap.cfg.RoleARN,
			func(o *stscreds.AssumeRoleOptions) {
				if ap.cfg.ExternalID != "" {
					o.ExternalID = &ap.cfg.ExternalID
				}

				if ap.cfg.RoleSessionName != "" {
					o.RoleSessionName = ap.cfg.RoleSessionName
				}
			})
		sdkCfg.Credentials = sdkaws.NewCredentialsCache(assume)
	}

	ap.client = secretsmanager.NewFromConfig(sdkCfg, func(o *secretsmanager.Options) {
		if ap.cfg.Endpoint != "" {
			o.BaseEndpoint = &ap.cfg.Endpoint
		}
	})

	return nil
}

// Get fetches a secret's payload and returns it verbatim — SecretString when
// present, otherwise the raw SecretBinary bytes. The store's version selector
// (version_stage/version_id) applies to every read; API errors are mapped to
// typed kv errors, and #field extraction belongs to the resolver.
func (ap *awsProvider) Get(ctx context.Context, key string) (string, error) {
	if err := ap.validateSecretKey(key); err != nil {
		return "", err
	}

	in := &secretsmanager.GetSecretValueInput{SecretId: &key}

	if ap.cfg.VersionID != "" {
		in.VersionId = &ap.cfg.VersionID
	} else if ap.cfg.VersionStage != "" {
		in.VersionStage = &ap.cfg.VersionStage
	}

	out, err := ap.client.GetSecretValue(ctx, in)
	if err != nil {
		return "", ap.classify(key, err)
	}

	var payload string

	switch {
	case out.SecretString != nil:
		payload = *out.SecretString
	case out.SecretBinary != nil:
		payload = string(out.SecretBinary)
	default:
		return "", &kv.StoreUnavailableError{
			KeyPath: key,
			Err:     errors.New("aws: secret has neither SecretString nor SecretBinary"),
		}
	}

	if ap.cfg.TrimTrailingNewline {
		payload = strings.TrimSuffix(payload, "\n")
	}

	return payload, nil
}

// Set writes value as the new AWSCURRENT version, creating the secret when it
// does not exist yet and tolerating a concurrent create. Writes are refused on
// version-pinned stores — rotating "the previous version" is not meaningful.
// Set bypasses the SecretStore wrapper, so it enforces its own deadline.
func (ap *awsProvider) Set(ctx context.Context, key, value string) error {
	if err := ap.validateSecretKey(key); err != nil {
		return err
	}

	if ap.cfg.VersionID != "" || (ap.cfg.VersionStage != "" && ap.cfg.VersionStage != "AWSCURRENT") {
		return fmt.Errorf("aws: cannot write %q through a version-pinned store", key)
	}

	ctx, cancel := context.WithTimeout(ctx, ap.setTimeout())
	defer cancel()

	err := ap.putSecretValue(ctx, key, value)
	if err == nil {
		return nil
	}

	// Not found is not an error in this case, it means the process should
	// create the secret first.
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return ap.classify(key, err)
	}

	_, err = ap.client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         &key,
		SecretString: &value,
	})
	if err == nil {
		return nil
	}

	// Another writer won the create race; the secret exists now, so retry the put.
	var exists *types.ResourceExistsException
	if !errors.As(err, &exists) {
		return ap.classify(key, err)
	}

	if err := ap.putSecretValue(ctx, key, value); err != nil {
		return ap.classify(key, err)
	}

	return nil
}

func (ap *awsProvider) putSecretValue(ctx context.Context, key, value string) error {
	_, err := ap.client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     &key,
		SecretString: &value,
	})

	return err
}

// Timeout is the per-call bound the SecretStore wrapper applies to Get; 0 means
// the wrapper's default.
func (ap *awsProvider) Timeout() time.Duration {
	return ap.timeout
}

func (ap *awsProvider) setTimeout() time.Duration {
	const defaultTimeout = 5 * time.Second

	if ap.timeout > 0 {
		return ap.timeout
	}

	return defaultTimeout
}

// validateSecretKey rejects keys that can never resolve: empty keys and ARNs
// that are malformed, belong to another service, or point outside the store's
// configured region. Cross-account ARNs are deliberately allowed — IAM decides.
func (ap *awsProvider) validateSecretKey(key string) error {
	if key == "" {
		return errors.New("aws: empty secret key")
	}

	if !strings.HasPrefix(key, "arn:") {
		return nil
	}

	a, err := arn.Parse(key)
	if err != nil {
		return fmt.Errorf("aws: invalid ARN key %q: %w", key, err)
	}

	if a.Service != "secretsmanager" {
		return fmt.Errorf("aws: key %q is not a secretsmanager ARN", key)
	}

	if a.Region != ap.cfg.Region {
		return fmt.Errorf("aws: key ARN region %q does not match store region %q", a.Region, ap.cfg.Region)
	}

	return nil
}

func (ap *awsProvider) classify(key string, err error) error {
	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return &kv.KeyNotFoundError{KeyPath: key}
	}

	var invalidParam *types.InvalidParameterException

	var invalidReq *types.InvalidRequestException

	if errors.As(err, &invalidParam) || errors.As(err, &invalidReq) {
		return fmt.Errorf("aws: invalid secret request for %q: %w", key, err)
	}

	return &kv.StoreUnavailableError{KeyPath: key, Err: err}
}
