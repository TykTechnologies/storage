package iamauth

import (
	"context"
	"testing"

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
