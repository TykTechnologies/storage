package file_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newProvider builds the provider through its factory with the given base_path,
// exactly as the registry would.
func newProvider(t *testing.T, basePath string) kv.Provider {
	t.Helper()

	cfg, err := json.Marshal(file.Config{BasePath: basePath})
	require.NoError(t, err)

	p, err := file.NewFactory()(cfg)
	require.NoError(t, err)
	require.NotNil(t, p)

	return p
}

func TestNewFactory(t *testing.T) {
	t.Parallel()

	t.Run("valid config builds a provider", func(t *testing.T) {
		t.Parallel()

		p, err := file.NewFactory()(json.RawMessage(`{"base_path":"/etc/tyk/secrets"}`))
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("empty config builds a provider (file refs rejected until base_path is set)", func(t *testing.T) {
		t.Parallel()

		for _, cfg := range []json.RawMessage{nil, {}, json.RawMessage(`{}`)} {
			p, err := file.NewFactory()(cfg)
			require.NoError(t, err, "config %q", string(cfg))
			require.NotNil(t, p)
		}
	})

	t.Run("invalid JSON config errors", func(t *testing.T) {
		t.Parallel()

		_, err := file.NewFactory()(json.RawMessage(`{not json`))
		require.Error(t, err)
	})
}

func TestProviderIsStandalone(t *testing.T) {
	t.Parallel()

	p := newProvider(t, "")

	standalone, ok := kv.AsStandalone(p)
	require.True(t, ok, "file provider must implement Standalone")
	require.True(t, standalone.IsStandalone(),
		"file reads are cheap and must reflect rotation immediately — no cache wrapper")
}

func TestProviderGet(t *testing.T) {
	t.Parallel()

	t.Run("strips a trailing LF newline", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("my-secret-value\n"), 0o600))

		got, err := newProvider(t, dir).Get(t.Context(), "secret.txt")
		require.NoError(t, err)
		assert.Equal(t, "my-secret-value", got)
	})

	t.Run("strips a trailing CRLF newline", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("my-secret-value\r\n"), 0o600))

		got, err := newProvider(t, dir).Get(t.Context(), "secret.txt")
		require.NoError(t, err)
		assert.Equal(t, "my-secret-value", got)
	})

	t.Run("preserves internal newlines, trims only the trailing one", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		pem := "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJ\n-----END CERTIFICATE-----\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cert.pem"), []byte(pem), 0o600))

		got, err := newProvider(t, dir).Get(t.Context(), "cert.pem")
		require.NoError(t, err)
		assert.Equal(t, "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJ\n-----END CERTIFICATE-----", got)
	})

	t.Run("missing relative key under base_path returns kv.KeyNotFoundError", func(t *testing.T) {
		t.Parallel()

		// Cross-provider consistency: an absent key is a not-found, detectable
		// via errors.As regardless of which provider produced it.
		dir := t.TempDir() // exists, but contains no "absent" file
		_, err := newProvider(t, dir).Get(t.Context(), "absent")

		var notFound *kv.KeyNotFoundError
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("base_path is mandatory: every key is rejected when it is empty", func(t *testing.T) {
		t.Parallel()

		for _, key := range []string{"my-cert", "/etc/passwd"} {
			_, err := newProvider(t, "").Get(t.Context(), key)
			require.ErrorIs(t, err, file.ErrBasePathRequired, "key %q", key)
		}
	})

	t.Run("resolves a relative key under base_path", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "api-key"), []byte("the-api-key"), 0o600))

		got, err := newProvider(t, dir).Get(t.Context(), "api-key")
		require.NoError(t, err)
		assert.Equal(t, "the-api-key", got)
	})

	t.Run("rejects an absolute path when base_path is set", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		f := filepath.Join(dir, "secret")
		require.NoError(t, os.WriteFile(f, []byte("abs-value"), 0o600))

		_, err := newProvider(t, "/some/other/base").Get(t.Context(), f)
		require.ErrorIs(t, err, file.ErrAbsoluteRejected)
	})

	t.Run("rejects an absolute path even when it points inside base_path", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		f := filepath.Join(dir, "secret")
		require.NoError(t, os.WriteFile(f, []byte("abs-value"), 0o600))

		_, err := newProvider(t, dir).Get(t.Context(), f)
		require.ErrorIs(t, err, file.ErrAbsoluteRejected)
	})

	t.Run("rejects dotdot traversal when base_path is set", func(t *testing.T) {
		t.Parallel()

		_, err := newProvider(t, t.TempDir()).Get(t.Context(), "../etc/passwd")
		require.ErrorIs(t, err, file.ErrTraversal)
	})

	t.Run("rejects embedded dotdot traversal", func(t *testing.T) {
		t.Parallel()

		_, err := newProvider(t, t.TempDir()).Get(t.Context(), "subdir/../../etc/passwd")
		require.ErrorIs(t, err, file.ErrTraversal)
	})

	t.Run("rejects a symlink that escapes base_path after EvalSymlinks", func(t *testing.T) {
		t.Parallel()

		base := t.TempDir()
		target := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(target, "passwd"), []byte("root:x:0:0"), 0o600))
		// A symlink inside base/ pointing outside to target/passwd.
		require.NoError(t, os.Symlink(filepath.Join(target, "passwd"), filepath.Join(base, "evil-link")))

		_, err := newProvider(t, base).Get(t.Context(), "evil-link")
		require.ErrorIs(t, err, file.ErrSymlinkEscape)
	})

	t.Run("follows K8s AtomicWriter symlinks", func(t *testing.T) {
		t.Parallel()

		// Simulate a K8s secret mount:
		//   <dir>/..2024_01_01_00_00_00/my-key   (actual data)
		//   <dir>/..data -> ..2024_01_01_00_00_00
		//   <dir>/my-key -> ..data/my-key
		dir := t.TempDir()
		dataDir := filepath.Join(dir, "..2024_01_01_00_00_00")
		require.NoError(t, os.Mkdir(dataDir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "my-key"), []byte("secret-from-k8s"), 0o600))
		require.NoError(t, os.Symlink("..2024_01_01_00_00_00", filepath.Join(dir, "..data")))
		require.NoError(t, os.Symlink("..data/my-key", filepath.Join(dir, "my-key")))

		got, err := newProvider(t, dir).Get(t.Context(), "my-key")
		require.NoError(t, err)
		assert.Equal(t, "secret-from-k8s", got)
	})
}

// TestProviderGet_PicksUpRotation proves the standalone (no-cache) contract:
// after a K8s AtomicWriter symlink swap, the very next Get returns the new
// value with no invalidation step.
func TestProviderGet_PicksUpRotation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	v1 := filepath.Join(dir, "..2024_01_01_00_00_00")
	require.NoError(t, os.Mkdir(v1, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(v1, "my-key"), []byte("old-secret"), 0o600))
	require.NoError(t, os.Symlink("..2024_01_01_00_00_00", filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink("..data/my-key", filepath.Join(dir, "my-key")))

	provider := newProvider(t, dir)

	got, err := provider.Get(t.Context(), "my-key")
	require.NoError(t, err)
	require.Equal(t, "old-secret", got)

	// Rotate: new version dir, atomic swap of the ..data symlink.
	v2 := filepath.Join(dir, "..2024_06_01_00_00_00")
	require.NoError(t, os.Mkdir(v2, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(v2, "my-key"), []byte("new-secret"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink("..2024_06_01_00_00_00", filepath.Join(dir, "..data")))

	got, err = provider.Get(t.Context(), "my-key")
	require.NoError(t, err)
	require.Equal(t, "new-secret", got, "standalone provider must reflect rotation on the next read")
}
