package redisv9

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/helper"
	"github.com/TykTechnologies/storage/temporal/internal/tlsconfig"
	"github.com/TykTechnologies/storage/temporal/temperr"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/redis/go-redis/v9"
)

type RedisV9 struct {
	connector model.Connector
	client    redis.UniversalClient

	cfg       *model.RedisOptions
	onConnect func(context.Context) error
	retryCfg  *model.RetryOptions
}

// NewList returns a new RedisV9 instance.
func NewRedisV9WithOpts(options ...model.Option) (*RedisV9, error) {
	baseConfig := &model.BaseConfig{}
	for _, opt := range options {
		opt.Apply(baseConfig)
	}

	universalOpts, err := buildUniversalOptions(baseConfig)
	if err != nil {
		return nil, err
	}

	opts := baseConfig.RedisConfig
	driver := &RedisV9{cfg: opts}

	if baseConfig.RetryConfig != nil {
		driver.retryCfg = baseConfig.RetryConfig
	}

	if baseConfig.OnConnect != nil {
		driver.onConnect = baseConfig.OnConnect
	}

	var client redis.UniversalClient

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

// buildUniversalOptions maps a BaseConfig into go-redis UniversalOptions. It is
// kept separate from client construction so the option mapping (in particular
// the credentials-provider wiring) can be unit tested without a live Redis.
func buildUniversalOptions(baseConfig *model.BaseConfig) (*redis.UniversalOptions, error) {
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

	if baseConfig.TLS != nil && baseConfig.TLS.Enable {
		var err error
		tlsConfig, err = tlsconfig.HandleTLS(baseConfig.TLS)
		if err != nil {
			return nil, err
		}
	}

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
		ConnMaxIdleTime:  240 * timeout,
		PoolSize:         poolSize,
		TLSConfig:        tlsConfig,
	}

	if baseConfig.RetryConfig != nil {
		universalOpts.MaxRetries = baseConfig.RetryConfig.MaxRetries
		universalOpts.MinRetryBackoff = baseConfig.RetryConfig.MinRetryBackoff
		universalOpts.MaxRetryBackoff = baseConfig.RetryConfig.MaxRetryBackoff
	}

	if baseConfig.OnConnect != nil {
		universalOpts.OnConnect = func(ctx context.Context, _ *redis.Conn) error {
			return baseConfig.OnConnect(ctx)
		}
	}

	// A credentials provider supplies rotating, short-lived credentials (e.g.
	// cloud IAM auth tokens) on each new connection. It takes precedence over
	// the static username/password, which are cleared to avoid ambiguity.
	if baseConfig.CredentialsProvider != nil {
		provider := baseConfig.CredentialsProvider
		universalOpts.CredentialsProviderContext = func(ctx context.Context) (string, string, error) {
			return provider(ctx)
		}
		universalOpts.Username = ""
		universalOpts.Password = ""
	}

	return universalOpts, nil
}

// NewRedisV9WithConnection returns a new redisv8List instance with a custom redis connection.
func NewRedisV9WithConnection(conn model.Connector) (*RedisV9, error) {
	var client redis.UniversalClient
	if conn == nil || !conn.As(&client) {
		return nil, temperr.InvalidConnector
	}

	return &RedisV9{connector: conn, client: client}, nil
}
