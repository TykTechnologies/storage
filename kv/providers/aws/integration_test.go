package aws_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/providers/aws"
	"github.com/stretchr/testify/require"
)

// integrationProvider builds a provider against a running AWS API emulator
// (moto server, or LocalStack with a license — Secrets Manager is Pro-gated
// in current LocalStack images). The suite is skipped unless TEST_AWS_ENDPOINT
// is set (e.g. http://127.0.0.1:15000), keeping the default `go test` run
// network-free.
func integrationProvider(t *testing.T) kv.Provider {
	t.Helper()

	endpoint := os.Getenv("TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_AWS_ENDPOINT not set; skipping emulator integration test")
	}

	cfg := fmt.Sprintf(`{
		"region": "us-east-1",
		"endpoint": %q,
		"access_key_id": "test",
		"secret_access_key": "test",
		"timeout": "10s"
	}`, endpoint)

	p, err := aws.NewFactory()(json.RawMessage(cfg))
	require.NoError(t, err)

	initializer, ok := kv.AsInitializer(p)
	require.True(t, ok)
	require.NoError(t, initializer.Init(t.Context()))

	return p
}

func TestIntegration_SetGetRoundTrip(t *testing.T) {
	t.Parallel()

	p := integrationProvider(t)

	setter, ok := kv.AsSetter(p)
	require.True(t, ok)

	key := fmt.Sprintf("tyk/test/round-trip-%d", time.Now().UnixNano())
	const value = `{"password":"s3cr3t"}`

	// First Set exercises the create-on-missing path against the real API.
	require.NoError(t, setter.Set(t.Context(), key, value))

	got, err := p.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	// Second Set exercises PutSecretValue on the now-existing secret.
	require.NoError(t, setter.Set(t.Context(), key, "rotated"))

	got, err = p.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, "rotated", got)
}

func TestIntegration_MissingKeyIsNotFound(t *testing.T) {
	t.Parallel()

	p := integrationProvider(t)

	_, err := p.Get(t.Context(), "tyk/test/definitely-missing")

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, err, &notFound)
}
