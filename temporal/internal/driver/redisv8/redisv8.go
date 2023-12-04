package redisv8

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/helper"
	"github.com/TykTechnologies/storage/temporal/temperr"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	connector model.Connector
	client    redis.UniversalClient

	cfg       *model.RedisOptions
	onConnect func(context.Context) error
	retryCfg  *model.RetryOptions
}

// NewList returns a new redisv8List instance.
func NewRedisV8WithOpts(options ...model.Option) (*RedisV8, error) {
	baseConfig := &model.BaseConfig{}
	for _, opt := range options {
		opt.Apply(baseConfig)
	}

	opts := baseConfig.RedisConfig
	if opts == nil {
		return nil, temperr.InvalidOptionsType
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

	driver := &RedisV8{cfg: opts}

	if baseConfig.RetryConfig != nil {
		driver.retryCfg = baseConfig.RetryConfig

		universalOpts.MaxRetries = baseConfig.RetryConfig.MaxRetries
		universalOpts.MinRetryBackoff = baseConfig.RetryConfig.MinRetryBackoff
		universalOpts.MaxRetryBackoff = baseConfig.RetryConfig.MaxRetryBackoff
	}

	if baseConfig.OnConnect != nil {
		driver.onConnect = baseConfig.OnConnect

		universalOpts.OnConnect = func(ctx context.Context, conn *redis.Conn) error {
			return baseConfig.OnConnect(ctx)
		}
	}

	switch {
	case opts.MasterName != "":
		client = redis.NewFailoverClient(universalOpts.Failover())
	case opts.EnableCluster:
		client = redis.NewClusterClient(universalOpts.Cluster())
	default:
		client = redis.NewClient(universalOpts.Simple())
	}

	driver.client = client

	return driver, nil
}

// NewRedisV8WithConnection returns a new redisv8List instance with a custom redis connection.
func NewRedisV8WithConnection(conn model.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if conn == nil || !conn.As(&client) {
		return nil, temperr.InvalidConnector
	}

	return &RedisV8{connector: conn, client: client}, nil
}
