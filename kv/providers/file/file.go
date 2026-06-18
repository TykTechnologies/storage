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

// Config is the file provider's configuration.
type Config struct {
	// BasePath, when set, is a security boundary: keys must be relative paths
	// confined within it; absolute paths and ".." traversal are rejected.
	// When empty, only absolute keys are accepted (no base to resolve against).
	BasePath string `json:"base_path"`
}

// NewFactory returns a ProviderFactory for filesystem-backed stores.
func NewFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		// Empty config is accepted because config doesn't have mandatory
		// fields.
		if len(config) == 0 {
			return &fileProvider{}, nil
		}

		var cfg Config

		err := json.Unmarshal(config, &cfg)
		if err != nil {
			return nil, err
		}

		return &fileProvider{basePath: cfg.BasePath}, nil
	}
}

type fileProvider struct {
	basePath string
}

func (fp *fileProvider) IsStandalone() bool {
	return true
}

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
	if fp.basePath != "" {
		canonicalBase, err := filepath.EvalSymlinks(fp.basePath)
		// EvalSymlinks failure here requires a race (basePath symlink broken between
		// resolving the file path above and this call). Not worth a flaky test.
		if err != nil {
			return "", fmt.Errorf("file: cannot resolve base_path %q: %w", fp.basePath, err)
		}

		if !confined(canonicalBase, resolved) {
			return "", fmt.Errorf("%w: key %q resolved to %q", ErrSymlinkEscape, key, resolved)
		}
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
		if !filepath.IsAbs(key) {
			return "", fmt.Errorf(
				"%w: key %q (set base_path or use an absolute path)",
				ErrBasePathRequired,
				key,
			)
		}

		return key, nil
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
