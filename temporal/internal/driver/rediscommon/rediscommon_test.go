package rediscommon

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestNewCommonRedisConfig(t *testing.T) {
	testCases := []struct {
		name           string
		clientOpts     *types.ClientOpts
		expectedConfig *CommonRedisConfig
	}{
		{
			name: "Default Config",
			clientOpts: &types.ClientOpts{
				Redis: &types.RedisOptions{},
			},
			expectedConfig: &CommonRedisConfig{
				PoolSize:     500,
				DialTimeout:  5 * time.Second,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
				IdleTimeout:  240 * (5 * time.Second),
				TLSConfig:    nil,
			},
		},
		{
			name: "Custom Pool Size",
			clientOpts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					MaxActive: 100,
				},
			},
			expectedConfig: &CommonRedisConfig{
				PoolSize:     100,
				DialTimeout:  5 * time.Second,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
				IdleTimeout:  240 * (5 * time.Second),
				TLSConfig:    nil,
			},
		},
		{
			name: "Custom Timeout",
			clientOpts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					BaseStorageOptions: types.BaseStorageOptions{
						Timeout: 10,
					},
				},
			},
			expectedConfig: &CommonRedisConfig{
				PoolSize:     500,
				DialTimeout:  10 * time.Second,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 10 * time.Second,
				IdleTimeout:  240 * (10 * time.Second),
				TLSConfig:    nil,
			},
		},
		{
			name: "SSL Enabled",
			clientOpts: &types.ClientOpts{
				Redis: &types.RedisOptions{
					BaseStorageOptions: types.BaseStorageOptions{
						UseSSL:                true,
						SSLInsecureSkipVerify: true,
					},
				},
			},
			expectedConfig: &CommonRedisConfig{
				PoolSize:     500,
				DialTimeout:  5 * time.Second,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
				IdleTimeout:  240 * (5 * time.Second),
				TLSConfig:    &tls.Config{InsecureSkipVerify: true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewCommonRedisConfig(tc.clientOpts)
			assert.Equal(t, tc.expectedConfig.PoolSize, result.PoolSize)
			assert.Equal(t, tc.expectedConfig.DialTimeout, result.DialTimeout)
			assert.Equal(t, tc.expectedConfig.ReadTimeout, result.ReadTimeout)
			assert.Equal(t, tc.expectedConfig.WriteTimeout, result.WriteTimeout)
			assert.Equal(t, tc.expectedConfig.IdleTimeout, result.IdleTimeout)
			if tc.expectedConfig.TLSConfig == nil {
				assert.Nil(t, result.TLSConfig)
			} else {
				assert.Equal(t, tc.expectedConfig.TLSConfig.InsecureSkipVerify, result.TLSConfig.InsecureSkipVerify)
			}
		})
	}
}
