package redis8

import (
	"crypto/tls"
	"strconv"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/go-redis/redis/v8"
)

type Redis8 struct {
	client redis.UniversalClient
}

func NewRedis8(opts *types.ClientOpts) *Redis8 {
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
		Addrs:            getRedisAddrs(opts.Redis),
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

	return &Redis8{client: client}
}

func getRedisAddrs(opts *types.RedisOptions) (addrs []string) {
	if len(opts.Addrs) != 0 {
		addrs = opts.Addrs
	} else {
		for h, p := range opts.Hosts {
			addr := h + ":" + p
			addrs = append(addrs, addr)
		}
	}

	if len(addrs) == 0 && opts.Port != 0 {
		addr := opts.Host + ":" + strconv.Itoa(opts.Port)
		addrs = append(addrs, addr)
	}

	return addrs
}
