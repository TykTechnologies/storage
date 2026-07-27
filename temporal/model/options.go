package model

import "context"

type Option interface {
	Apply(*BaseConfig)
}

type opts struct {
	fn func(*BaseConfig)
}

func (o *opts) Apply(bcfg *BaseConfig) {
	o.fn(bcfg)
}

// WithRedisConfig is a helper function to create a ConnectionOption for Redis.
func WithRedisConfig(config *RedisOptions) Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			bcfg.RedisConfig = config
		},
	}
}

// WithNoopConfig is a helper function to avoid creating a connection - useful for testing.
func WithNoopConfig() Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			// Empty function that does nothing.
		},
	}
}

// WithRetries is a helper function to create a RetryOption for the storage.
func WithRetries(config *RetryOptions) Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			bcfg.RetryConfig = config
		},
	}
}

// WithOnConnect is a helper function to trigger onConnect when a connection or reconnection
// is established.
func WithOnConnect(onConnect func(context.Context) error) Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			bcfg.OnConnect = onConnect
		},
	}
}

// WithCredentialsProvider supplies a function that returns the username and
// password for each new Redis connection, instead of static credentials. Use
// this for rotating, short-lived credentials such as cloud IAM auth tokens
// (e.g. GCP Memorystore for Valkey, AWS ElastiCache). When set, it takes
// precedence over RedisOptions.Username/Password.
func WithCredentialsProvider(provider CredentialsProviderFunc) Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			bcfg.CredentialsProvider = provider
		},
	}
}

// WithTLS is a helper function to create a TLSOption for the storage.
func WithTLS(config *TLS) Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
			bcfg.TLS = config
		},
	}
}
