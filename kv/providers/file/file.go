// Package file provides a KV provider that reads secrets from the local
// filesystem — plain files and Kubernetes Secrets mounted as files. It is a
// Standalone provider (no caching) so rotated secrets are visible on the next read.
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TykTechnologies/storage/kv"
)

// Config is used to configure a local-file store.
type Config struct {
	// BasePath is the directory the store reads secrets from, and the boundary
	// that keys are not allowed to leave. A common use is a directory of
	// Kubernetes Secrets mounted into the Tyk container, where each secret shows
	// up as a file whose contents are the value.
	//
	// Keys are paths relative to this directory: with a base path of
	// "/etc/tyk/secrets", the reference kv://<store-name>/db/password reads the
	// file /etc/tyk/secrets/db/password. The value is the file's contents with any
	// trailing newlines removed.
	//
	// It must be an absolute path. A relative one is refused when the store is
	// created, so the location can never depend on which directory Tyk happened
	// to be started from. Keys that try to escape the directory are refused too:
	// absolute paths, paths containing "..", and symbolic links that lead outside
	// it. This is what stops a reference from reaching arbitrary files on the
	// host, which matters because references are written in API definitions, and
	// those may come from people less trusted than whoever runs Tyk.
	//
	// Leaving it empty switches the store off: every read fails until a base path
	// is configured.
	BasePath string `json:"base_path"`
}

// NewFactory returns a ProviderFactory for filesystem-backed stores.
//
// An empty or absent config is accepted and yields a provider with no
// base_path; that provider rejects every Get (see Config.BasePath). This lets
// callers register the store unconditionally and treat "no base_path" as
// "file references disabled" rather than a construction error.
func NewFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		if len(config) == 0 {
			return &fileProvider{}, nil
		}

		var cfg Config

		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("file: invalid config: %w", err)
		}

		if cfg.BasePath != "" && !filepath.IsAbs(cfg.BasePath) {
			return nil, fmt.Errorf("%w: %q", ErrBasePathNotAbsolute, cfg.BasePath)
		}

		return &fileProvider{basePath: cfg.BasePath}, nil
	}
}

type fileProvider struct {
	basePath string
}

// IsStandalone reports that the provider needs no cache wrapper: filesystem
// reads are cheap.
func (fp *fileProvider) IsStandalone() bool {
	return true
}

// Get reads the file addressed by key and returns its contents with trailing
// newlines trimmed. key must be a relative path confined to base_path (see
// Config.BasePath). A missing file returns *kv.KeyNotFoundError.
func (fp *fileProvider) Get(ctx context.Context, key string) (string, error) {
	path, err := resolveKeyPath(fp.basePath, key)
	if err != nil {
		return "", err
	}

	// Resolve K8s AtomicWriter symlinks (e.g. ..data -> ..2024_01_01_00_00_00).
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", &kv.KeyNotFoundError{KeyPath: key}
		}

		return "", fmt.Errorf("file: cannot resolve path %q: %w", path, err)
	}

	// Re-verify after symlink resolution: a symlink inside basePath can point
	// outside (symlink escape).
	canonicalBase, err := filepath.EvalSymlinks(fp.basePath)
	// EvalSymlinks failure here requires a race (basePath symlink broken between
	// resolving the file path above and this call). Not worth a flaky test.
	if err != nil {
		return "", fmt.Errorf("file: cannot resolve base_path %q: %w", fp.basePath, err)
	}

	if !confined(canonicalBase, resolved) {
		return "", fmt.Errorf("%w: key %q resolved to %q", ErrSymlinkEscape, key, resolved)
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", &kv.KeyNotFoundError{KeyPath: key}
		}

		return "", fmt.Errorf("file: cannot read file %q: %w", resolved, err)
	}

	return strings.TrimRight(string(data), "\r\n"), nil
}

// resolveKeyPath applies the base_path boundary policy and returns the
// candidate file path.
func resolveKeyPath(basePath, key string) (string, error) {
	if basePath == "" {
		return "", fmt.Errorf(
			"%w: key %q (set base_path)",
			ErrBasePathRequired,
			key,
		)
	}

	if key == "" {
		return "", ErrEmptyKey
	}

	if filepath.IsAbs(key) {
		return "", fmt.Errorf(
			"%w: %q (use a path relative to base_path)",
			ErrAbsoluteRejected,
			key,
		)
	}

	joined := filepath.Join(basePath, key)
	if !confined(basePath, joined) {
		return "", fmt.Errorf("%w: key %q", ErrTraversal, key)
	}

	return joined, nil
}

// confined reports whether target resolves to a location within base,
// using lexical analysis only.
func confined(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	return err == nil && filepath.IsLocal(rel)
}
