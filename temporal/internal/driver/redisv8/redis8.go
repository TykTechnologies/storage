package redisv8

import (
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/TykTechnologies/storage/temporal/utils"
	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	client redis.UniversalClient
}

func NewRedisV8(opts *types.ClientOpts) *RedisV8 {
	// poolSize applies per cluster node and not for the whole cluster.
	poolSize := 500
	if opts.Redis.MaxActive > 0 {
		poolSize = opts.Redis.MaxActive
	}

	timeout := 5 * time.Second
	if opts.Redis.Timeout != 0 {
		timeout = time.Duration(opts.Redis.Timeout) * time.Second
	}

	var tlsConfig *tls.Config
	if opts.Redis.UseSSL {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: opts.Redis.SSLInsecureSkipVerify,
		}
	}
	var client redis.UniversalClient

	universalOpts := &redis.UniversalOptions{
		Addrs:            utils.GetRedisAddrs(opts.Redis),
		MasterName:       opts.Redis.MasterName,
		SentinelPassword: opts.Redis.SentinelPassword,
		Username:         opts.Redis.Username,
		Password:         opts.Redis.Password,
		DB:               opts.Redis.Database,
		DialTimeout:      timeout,
		ReadTimeout:      timeout,
		WriteTimeout:     timeout,
		IdleTimeout:      240 * timeout,
		PoolSize:         poolSize,
		TLSConfig:        tlsConfig,
	}

	switch {
	case opts.Redis.MasterName != "":
		client = redis.NewFailoverClient(universalOpts.Failover())
	case opts.Redis.EnableCluster:
		client = redis.NewClusterClient(universalOpts.Cluster())
	default:
		client = redis.NewClient(universalOpts.Simple())
	}

	return &RedisV8{client: client}
}
