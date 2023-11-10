package redisv8

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/rediscommon"
	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	client redis.UniversalClient
}

func NewRedisV8(opts *types.ClientOpts) *RedisV8 {
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
		IdleTimeout:      commonConfig.IdleTimeout,
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

	return &RedisV8{client: client}
}
