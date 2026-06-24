package redisv9

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildUniversalOptions_StaticCredentials(t *testing.T) {
	baseConfig := &model.BaseConfig{
		RedisConfig: &model.RedisOptions{
			Host:     "localhost",
			Port:     6379,
			Username: "user",
			Password: "static-pass",
		},
	}

	opts, err := buildUniversalOptions(baseConfig)
	require.NoError(t, err)

	assert.Equal(t, "user", opts.Username)
	assert.Equal(t, "static-pass", opts.Password)
	assert.Nil(t, opts.CredentialsProviderContext)
}

func TestBuildUniversalOptions_CredentialsProvider(t *testing.T) {
	baseConfig := &model.BaseConfig{
		RedisConfig: &model.RedisOptions{
			Host:     "localhost",
			Port:     6379,
			Username: "ignored-user",
			Password: "ignored-pass",
		},
		CredentialsProvider: func(_ context.Context) (string, string, error) {
			return "default", "iam-token", nil
		},
	}

	opts, err := buildUniversalOptions(baseConfig)
	require.NoError(t, err)

	require.NotNil(t, opts.CredentialsProviderContext, "provider must be wired to CredentialsProviderContext")

	username, password, err := opts.CredentialsProviderContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "default", username)
	assert.Equal(t, "iam-token", password)

	// Static credentials must be cleared so they don't override the provider.
	assert.Empty(t, opts.Username, "static username should be cleared when a provider is set")
	assert.Empty(t, opts.Password, "static password should be cleared when a provider is set")
}
