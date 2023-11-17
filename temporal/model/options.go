package model

type Option interface {
	Apply(*BaseConfig)
}

type opts struct {
	fn func(*BaseConfig)
}

func (o *opts) Apply(bcfg *BaseConfig) {
	o.fn(bcfg)
}

const (
	RedisV8Type = "redisv8"
)

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
