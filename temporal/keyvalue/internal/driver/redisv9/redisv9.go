package redisv9

import (
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/driver/rediscommon"
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/types"
	"github.com/redis/go-redis/v9"
)

type RedisV9 struct {
	client redis.UniversalClient
}

func NewRedisV9(opts *types.ClientOpts) *RedisV9 {
	commonConfig := rediscommon.NewCommonRedisConfig(opts)

	universalOpts := &redis.UniversalOptions{
		Addrs:            commonConfig.Addrs,
		MasterName:       commonConfig.MasterName,
		SentinelPassword: commonConfig.SentinelPassword,
		Username:         commonConfig.Username,
		Password:         commonConfig.Password,
		DB:               commonConfig.DB,
		DialTimeout:      commonConfig.DialTimeout,
		ReadTimeout:      commonConfig.ReadTimeout,
		WriteTimeout:     commonConfig.WriteTimeout,
		ConnMaxIdleTime:  commonConfig.IdleTimeout,
		PoolSize:         commonConfig.PoolSize,
		TLSConfig:        commonConfig.TLSConfig,
	}

	var client redis.UniversalClient

	switch {
	case opts.Redis.MasterName != "":
		client = redis.NewFailoverClient(universalOpts.Failover())
	case opts.Redis.EnableCluster:
		client = redis.NewClusterClient(universalOpts.Cluster())
	default:
		client = redis.NewClient(universalOpts.Simple())
	}

	return &RedisV9{client: client}
}
