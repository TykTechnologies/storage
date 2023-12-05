package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	tcs := []struct {
		name            string
		givenOption     Option
		expectedBaseCfg *BaseConfig
	}{
		{
			name: "WithRedisConfig",
			givenOption: WithRedisConfig(&RedisOptions{
				Username: "test",
				Password: "test",
				Host:     "test",
				Port:     1234,
				Timeout:  1234,
				Hosts: map[string]string{
					"test": "test",
				},
				Addrs:            []string{"test"},
				MasterName:       "test",
				SentinelPassword: "test",
				Database:         1234,
				MaxActive:        1234,
				EnableCluster:    true,
			}),
			expectedBaseCfg: &BaseConfig{
				RedisConfig: &RedisOptions{
					Username: "test",
					Password: "test",
					Host:     "test",
					Port:     1234,
					Timeout:  1234,
					Hosts: map[string]string{
						"test": "test",
					},
					Addrs:            []string{"test"},
					MasterName:       "test",
					SentinelPassword: "test",
					Database:         1234,
					MaxActive:        1234,
					EnableCluster:    true,
				},
			},
		},
		{
			name:        "WithNoopConfig",
			givenOption: WithNoopConfig(),
			expectedBaseCfg: &BaseConfig{
				RedisConfig: nil,
			},
		},
		{
			name: "WithReconnect",
			givenOption: WithRetries(&RetryOptions{
				MaxRetries:      2,
				MinRetryBackoff: time.Duration(2),
				MaxRetryBackoff: time.Duration(2),
			}),
			expectedBaseCfg: &BaseConfig{
				RetryConfig: &RetryOptions{
					MaxRetries:      2,
					MinRetryBackoff: time.Duration(2),
					MaxRetryBackoff: time.Duration(2),
				},
			},
		},
		{
			name:        "WithOnConnect",
			givenOption: WithOnConnect(nil),
			expectedBaseCfg: &BaseConfig{
				OnConnect: nil,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			baseCfg := &BaseConfig{}
			tc.givenOption.Apply(baseCfg)
			assert.Equal(t, tc.expectedBaseCfg, baseCfg)
		})
	}
}
