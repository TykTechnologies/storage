// Package vault provides a kv.Provider backed by HashiCorp Vault. It reads
// secrets from Vault's KV engine (v1 or v2) and returns each secret's data map
// serialized as JSON, leaving field selection to the resolver's "#field" syntax.
//
// Unlike the local providers (env, file, inline), vault is remote: it is not
// Standalone and exposes a Timeouter, so the registry wraps it in the caching /
// singleflight SecretStore. The Vault client is created without any network I/O;
// the connection is established lazily on the first Get.
package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/kv"
	vaultsdk "github.com/hashicorp/vault/api"
)

// vaultProvider is a kv.Provider backed by a configured Vault API client.
type vaultProvider struct {
	// client is the Vault API client. The resolved Config (address, token,
	// retries, timeout) is already baked into it at construction.
	client *vaultsdk.Client

	// timeout is the parsed Config.Timeout, surfaced via Timeout() so the
	// SecretStore wrapper can bound each Get with it. 0 means "unset", letting
	// the store use its own default.
	timeout time.Duration

	// kvv2 selects the read path: when true, Get injects "/data" after the
	// mount and unwraps the v2 "data" envelope; when false it reads the path
	// as-is (KV v1).
	kvv2 bool

	// mountPath is the normalized (trailing slash trimmed) Config.MountPath. When
	// non-empty it overrides the legacy first-segment "/data" injection for KV v2
	// and confines keys to that mount.
	mountPath string
}

// Get reads the secret at key and returns its data map serialized as JSON.
// Field selection (the "#field" fragment) is the resolver's responsibility, so
// Get returns the whole secret, never a single value. When mount_path is
// configured, key must be the full logical path under that mount.
//
// A missing secret returns *kv.KeyNotFoundError; a backend or transport failure
// returns *kv.StoreUnavailableError.
func (vp *vaultProvider) Get(ctx context.Context, key string) (string, error) {
	apiPath, err := vp.physicalPath(key)
	if err != nil {
		return "", err
	}

	secret, err := vp.client.Logical().ReadWithContext(ctx, apiPath)
	if err != nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	if secret == nil {
		return "", &kv.KeyNotFoundError{KeyPath: key}
	}

	data := secret.Data

	if vp.kvv2 {
		var ok bool
		// KV v2 wraps the secret in an inner "data" field. Its absence means the
		// path holds no v2 secret, which we treat as not-found.
		data, ok = data["data"].(map[string]any)
		if !ok {
			return "", &kv.KeyNotFoundError{KeyPath: key}
		}
	}

	b, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("vault: encode secret %q: %w", key, err)
	}

	return string(b), nil
}

// Set writes value as the secret's data at key. For KV v2 the map is wrapped in the
// "data" envelope and "/data" is injected into the path; for KV v1 the map
// is written as-is. When mount_path is set, key must be the full logical path under that mount.
//
// A value that is not a JSON object is rejected before any request; a backend or
// transport failure returns *kv.StoreUnavailableError.
func (vp *vaultProvider) Set(ctx context.Context, key, value string) error {
	apiPath, err := vp.physicalPath(key)
	if err != nil {
		return err
	}

	var fields map[string]any

	err = json.Unmarshal([]byte(value), &fields)
	if err != nil {
		return fmt.Errorf("vault: value must be a JSON object: %w", err)
	}

	if fields == nil {
		return errors.New("vault: value must be a JSON object")
	}

	data := fields
	if vp.kvv2 {
		data = map[string]any{"data": fields}
	}

	ctx, cancel := context.WithTimeout(ctx, kv.EffectiveTimeout(vp.timeout))
	defer cancel()

	_, err = vp.client.Logical().WriteWithContext(ctx, apiPath, data)
	if err != nil {
		return &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}

	return nil
}

func (vp *vaultProvider) Timeout() time.Duration {
	return vp.timeout
}

// physicalPath maps the caller's logical key to the physical path the Vault HTTP
// API expects. For KV v1 the logical and physical paths are identical. For KV v2
// the engine stores secrets under "<mount>/data/<path>", so the "/data/" segment
// is inserted after the mount:
//
//   - With mount_path set, immediately after that (possibly multi-segment) mount.
//     key must be the full logical path under the mount; otherwise it is rejected.
//     The check is on the path-segment boundary (mountPath + "/"), so a mount of
//     "secret" does not match an unrelated "secrets/..." key.
//   - With mount_path empty, after the first path segment — the legacy assumption
//     that the mount is a single top-level segment. The injection is intentionally
//     blind: a segment legitimately named "data" must not be special-cased.
func (vp *vaultProvider) physicalPath(key string) (string, error) {
	if !vp.kvv2 {
		return key, nil
	}

	if vp.mountPath != "" {
		clean := path.Clean(key)
		if !strings.HasPrefix(clean, vp.mountPath+"/") {
			return "", fmt.Errorf("vault: key %q is not under mount_path %q", key, vp.mountPath)
		}

		return vp.mountPath + "/data" + strings.TrimPrefix(clean, vp.mountPath), nil
	}

	splitted := strings.Split(key, "/")
	splitted[0] += "/data"

	return strings.Join(splitted, "/"), nil
}
