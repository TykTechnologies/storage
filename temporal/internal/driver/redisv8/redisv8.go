package redisv8

import (
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/helper"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	connector model.Connector
	client    redis.UniversalClient

	cfg *model.RedisOptions
}

// NewList returns a new redisv8List instance.
func NewRedisV8WithOpts(options ...model.Option) (*RedisV8, error) {
	baseConfig := &model.BaseConfig{}
	for _, opt := range options {
		opt.Apply(baseConfig)
	}

	opts := baseConfig.RedisConfig
	if opts == nil {
		return nil, model.ErrInvalidOptionsType
	}

	// poolSize applies per cluster node and not for the whole cluster.
	poolSize := 500
	if opts.MaxActive > 0 {
		poolSize = opts.MaxActive
	}

	timeout := 5 * time.Second
	if opts.Timeout != 0 {
		timeout = time.Duration(opts.Timeout) * time.Second
	}

	var tlsConfig *tls.Config
	if opts.UseSSL {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: opts.SSLInsecureSkipVerify,
		}
	}
	var client redis.UniversalClient

	universalOpts := &redis.UniversalOptions{
		Addrs:            helper.GetRedisAddrs(opts),
		MasterName:       opts.MasterName,
		SentinelPassword: opts.SentinelPassword,
		Username:         opts.Username,
		Password:         opts.Password,
		DB:               opts.Database,
		DialTimeout:      timeout,
		ReadTimeout:      timeout,
		WriteTimeout:     timeout,
		IdleTimeout:      240 * timeout,
		PoolSize:         poolSize,
		TLSConfig:        tlsConfig,
	}

	switch {
	case opts.MasterName != "":
		client = redis.NewFailoverClient(universalOpts.Failover())
	case opts.EnableCluster:
		client = redis.NewClusterClient(universalOpts.Cluster())
	default:
		client = redis.NewClient(universalOpts.Simple())
	}

	return &RedisV8{client: client, cfg: opts}, nil
}

// NewRedisV8WithConnection returns a new redisv8List instance with a custom redis connection.
func NewRedisV8WithConnection(conn model.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if conn == nil || !conn.As(&client) {
		return nil, model.ErrInvalidConnector
	}

	return &RedisV8{connector: conn, client: client}, nil
}
