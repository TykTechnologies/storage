package iamauth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProvider_MissingProvider(t *testing.T) {
	_, err := NewProvider(context.Background(), Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider")
}

func TestNewProvider_UnsupportedProvider(t *testing.T) {
	_, err := NewProvider(context.Background(), Config{Provider: "azure"})
	require.Error(t, err)
	// The error must name the offending provider and flag it as unsupported.
	assert.Contains(t, err.Error(), "azure")
	assert.Contains(t, err.Error(), "unsupported")
}

func TestParseRefreshBeforeExpiry(t *testing.T) {
	t.Run("empty yields zero and no error", func(t *testing.T) {
		d, err := parseRefreshBeforeExpiry("  ")
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), d)
	})

	t.Run("valid duration is parsed", func(t *testing.T) {
		d, err := parseRefreshBeforeExpiry("90s")
		require.NoError(t, err)
		assert.Equal(t, 90*time.Second, d)
	})

	t.Run("invalid duration errors with the offending value", func(t *testing.T) {
		_, err := parseRefreshBeforeExpiry("nope")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nope")
	})
}

func TestNewProvider_InvalidRefreshDuration(t *testing.T) {
	// A malformed duration string must be rejected up front — before any
	// credential resolution — with an error that names the offending value.
	_, err := NewProvider(context.Background(), Config{Provider: "gcp", RefreshBeforeExpiry: "not-a-duration"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not-a-duration")
	assert.Contains(t, err.Error(), "refresh_before_expiry")
}

func TestNewProvider_GCPRoutesToGCP(t *testing.T) {
	// Provider "gcp" must route to the gcp implementation. Whether ADC is
	// configured in this environment decides success vs. a credentials error,
	// but it must never be rejected as an unsupported provider.
	provider, err := NewProvider(context.Background(), Config{Provider: "gcp"})
	if err != nil {
		assert.NotContains(t, err.Error(), "unsupported")
		return
	}

	assert.NotNil(t, provider)
}
