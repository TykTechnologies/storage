package types

type BaseConfig struct {
	RedisConfig *RedisOptions
}

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

// WithNoopConfig is a helper function to avoid creating a connection - usefull for testing.
func WithNoopConfig() Option {
	return &opts{
		fn: func(bcfg *BaseConfig) {
		},
	}
}

// RedisOptions contains options specific to Redis storage.
type RedisOptions struct {
	// Connection username
	Username string `json:"username"`
	// Connection password
	Password string `json:"password"`
	// Connection host. For example: "localhost"
	Host string `json:"host"`
	// Connection port. For example: 6379
	Port int `json:"port"`
	// Set a custom timeout for Redis network operations. Default value 5 seconds.
	Timeout int `json:"timeout"`
	// Enable SSL/TLS connection between your Tyk Gateway & Redis.
	UseSSL bool `json:"use_ssl"`
	// Disable TLS verification
	SSLInsecureSkipVerify bool `json:"ssl_insecure_skip_verify"`

	Hosts map[string]string `json:"hosts"` // Deprecated: Addrs instead.
	// If you have multi-node setup, you should use this field instead. For example: ["host1:port1", "host2:port2"].
	Addrs []string `json:"addrs"`
	// Redis sentinel master name
	MasterName string `json:"master_name"`
	// Redis sentinel password
	SentinelPassword string `json:"sentinel_password"`
	// Redis database
	Database int `json:"database"`
	// Set the number of maximum connections in the Redis connection pool, which defaults to 500
	// Set to a higher value if you are expecting more traffic.
	MaxActive int `json:"optimisation_max_active"`
	// Enable Redis Cluster support
	EnableCluster bool `json:"enable_cluster"`
}
