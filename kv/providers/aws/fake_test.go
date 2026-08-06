package aws

import (
	"context"
	"fmt"
	"sync"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

// fakeVersion is one stored version of a fake secret. Exactly one of str or
// binary is normally set; both nil models a corrupt/empty payload.
type fakeVersion struct {
	id     string
	str    *string
	binary []byte
}

func strVersion(id, value string) fakeVersion {
	return fakeVersion{id: id, str: &value}
}

func binVersion(id string, data []byte) fakeVersion {
	return fakeVersion{id: id, binary: data}
}

// fakeSecretsManager is an in-process stand-in for the Secrets Manager API,
// injected through the provider's testClient seam. Versions are ordered:
// the last one carries AWSCURRENT, the one before it AWSPREVIOUS.
type fakeSecretsManager struct {
	mu      sync.Mutex
	secrets map[string][]fakeVersion

	// Optional per-call overrides. When non-nil, the hook replaces the default.
	getHook    func(context.Context, *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error)
	putHook    func(context.Context, *secretsmanager.PutSecretValueInput) (*secretsmanager.PutSecretValueOutput, error)
	createHook func(context.Context, *secretsmanager.CreateSecretInput) (*secretsmanager.CreateSecretOutput, error)

	getCalls, putCalls, createCalls int
}

func newFakeSecretsManager() *fakeSecretsManager {
	return &fakeSecretsManager{secrets: make(map[string][]fakeVersion)}
}

// seed appends one or more versions to a secret, creating it if absent.
func (f *fakeSecretsManager) seed(name string, versions ...fakeVersion) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.secrets[name] = append(f.secrets[name], versions...)
}

func (f *fakeSecretsManager) calls() (get, put, create int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.getCalls, f.putCalls, f.createCalls
}

func (f *fakeSecretsManager) GetSecretValue(
	ctx context.Context,
	in *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	f.mu.Lock()
	f.getCalls++
	hook := f.getHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, in)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	name := sdkaws.ToString(in.SecretId)

	versions, ok := f.secrets[name]
	if !ok || len(versions) == 0 {
		return nil, &types.ResourceNotFoundException{}
	}

	v, err := pickVersion(versions, in.VersionId, in.VersionStage)
	if err != nil {
		return nil, err
	}

	return &secretsmanager.GetSecretValueOutput{
		Name:         in.SecretId,
		VersionId:    &v.id,
		SecretString: v.str,
		SecretBinary: v.binary,
	}, nil
}

func pickVersion(versions []fakeVersion, versionID, versionStage *string) (fakeVersion, error) {
	if versionID != nil {
		for _, v := range versions {
			if v.id == *versionID {
				return v, nil
			}
		}

		return fakeVersion{}, &types.ResourceNotFoundException{}
	}

	stage := "AWSCURRENT"
	if versionStage != nil {
		stage = *versionStage
	}

	switch stage {
	case "AWSCURRENT":
		return versions[len(versions)-1], nil
	case "AWSPREVIOUS":
		if len(versions) < 2 {
			return fakeVersion{}, &types.ResourceNotFoundException{}
		}

		return versions[len(versions)-2], nil
	default:
		return fakeVersion{}, &types.ResourceNotFoundException{}
	}
}

func (f *fakeSecretsManager) PutSecretValue(
	ctx context.Context,
	in *secretsmanager.PutSecretValueInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.PutSecretValueOutput, error) {
	f.mu.Lock()
	f.putCalls++
	hook := f.putHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, in)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	name := sdkaws.ToString(in.SecretId)

	if _, ok := f.secrets[name]; !ok {
		return nil, &types.ResourceNotFoundException{}
	}

	id := fmt.Sprintf("v%d", len(f.secrets[name])+1)
	f.secrets[name] = append(f.secrets[name], fakeVersion{id: id, str: in.SecretString})

	return &secretsmanager.PutSecretValueOutput{Name: in.SecretId, VersionId: &id}, nil
}

func (f *fakeSecretsManager) CreateSecret(
	ctx context.Context,
	in *secretsmanager.CreateSecretInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.CreateSecretOutput, error) {
	f.mu.Lock()
	f.createCalls++
	hook := f.createHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, in)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	name := sdkaws.ToString(in.Name)

	if _, ok := f.secrets[name]; ok {
		return nil, &types.ResourceExistsException{}
	}

	f.secrets[name] = []fakeVersion{{id: "v1", str: in.SecretString}}

	return &secretsmanager.CreateSecretOutput{Name: in.Name}, nil
}
