package redisv9

import (
	"context"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
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

func TestBuildUniversalOptions_NilRedisConfig_Errors(t *testing.T) {
	_, err := buildUniversalOptions(&model.BaseConfig{})
	require.ErrorIs(t, err, temperr.InvalidOptionsType)
}

func TestBuildUniversalOptions_MapsPoolTimeoutRetryAndOnConnect(t *testing.T) {
	onConnectCalls := 0
	baseConfig := &model.BaseConfig{
		RedisConfig: &model.RedisOptions{
			Host:      "localhost",
			Port:      6379,
			MaxActive: 128,
			Timeout:   7,
		},
		RetryConfig: &model.RetryOptions{
			MaxRetries:      5,
			MinRetryBackoff: 10 * time.Millisecond,
			MaxRetryBackoff: 200 * time.Millisecond,
		},
		OnConnect: func(context.Context) error {
			onConnectCalls++

			return nil
		},
	}

	opts, err := buildUniversalOptions(baseConfig)
	require.NoError(t, err)

	assert.Equal(t, 128, opts.PoolSize)
	assert.Equal(t, 7*time.Second, opts.DialTimeout)
	assert.Equal(t, 5, opts.MaxRetries)
	assert.Equal(t, 10*time.Millisecond, opts.MinRetryBackoff)
	assert.Equal(t, 200*time.Millisecond, opts.MaxRetryBackoff)

	require.NotNil(t, opts.OnConnect)
	require.NoError(t, opts.OnConnect(context.Background(), nil))
	assert.Equal(t, 1, onConnectCalls, "OnConnect wrapper must invoke the configured callback")
}
