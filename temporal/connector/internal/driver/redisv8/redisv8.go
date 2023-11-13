package redisv8

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/connector/internal/helper"
	"github.com/TykTechnologies/storage/temporal/connector/types"

	"github.com/go-redis/redis/v8"
)

type Connector struct {
	client redis.UniversalClient
	cfg    *types.RedisOptions
}

func NewConnector(options ...types.Option) (*Connector, error) {
	baseConfig := &types.BaseConfig{}
	for _, opt := range options {
		opt.Apply(baseConfig)
	}

	opts := baseConfig.RedisConfig
	if opts == nil {
		return nil, types.ErrInvalidOptionsType
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

	return &Connector{client: client, cfg: opts}, nil
}

func (h *Connector) IsConnected() {

}

func (h *Connector) Disconnect(ctx context.Context) error {
	return h.client.Close()
}

func (h *Connector) Ping(ctx context.Context) error {
	return h.client.Ping(ctx).Err()
}

func (h *Connector) Type() string {
	return types.RedisV8Type
}

// As converts i to driver-specific types.
// redisv8 connector supports only *redis.UniversalClient.
func (h *Connector) As(con interface{}) bool {
	if x, ok := con.(*redis.UniversalClient); ok {
		*x = h.client
		return true
	}

	return false
}
