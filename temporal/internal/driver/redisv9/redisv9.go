package redisv9

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/helper"
	"github.com/TykTechnologies/storage/temporal/internal/tlsconfig"
	"github.com/TykTechnologies/storage/temporal/temperr"

	"github.com/redis/go-redis/v9"

	"github.com/TykTechnologies/storage/temporal/model"
)

type RedisV9 struct {
	connector model.Connector
	client    redis.UniversalClient

	cfg       *model.RedisOptions
	onConnect func(context.Context) error
	retryCfg  *model.RetryOptions

	// closed flips on Disconnect so PoolStats can error instead of reporting
	// counters for a closed pool (go-redis' PoolStats has no closed check).
	// The pointer comes from closedFlagFor, keyed by the client, so every
	// handler on the same client shares one flag no matter how it was
	// constructed — including through wrapper connectors that only surface
	// the client via As().
	closed *atomic.Bool
}

// closedFlags maps a live client to the closed flag shared by every handler
// built on it. It is keyed by the client — the resource whose lifecycle the
// flag tracks — rather than by connector identity, so a wrapper connector that
// delegates As() to an inner *RedisV9 still shares the inner flag. Disconnect
// removes the entry so closed clients stay collectable; existing handlers hold
// the flag pointer directly and keep observing true after removal.
var (
	closedFlagsMu sync.Mutex
	closedFlags   = map[redis.UniversalClient]*atomic.Bool{}
)

// closedFlagFor returns the shared closed flag for client, creating it on
// first use. Handlers built from an already-disconnected client get a fresh
// (open) flag; such handlers are misuse and fail on every command anyway.
func closedFlagFor(client redis.UniversalClient) *atomic.Bool {
	closedFlagsMu.Lock()
	defer closedFlagsMu.Unlock()

	flag, ok := closedFlags[client]
	if !ok {
		flag = &atomic.Bool{}
		closedFlags[client] = flag
	}

	return flag
}

// forgetClosedFlag drops the registry entry for client so a closed client can
// be garbage collected. Handlers keep their flag pointer and its value.
func forgetClosedFlag(client redis.UniversalClient) {
	closedFlagsMu.Lock()
	defer closedFlagsMu.Unlock()

	delete(closedFlags, client)
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
	driver.closed = closedFlagFor(client)

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

	// The flag is looked up by client, so it is shared with whichever
	// connector owns this client — even when conn is a wrapper and not a
	// *RedisV9 itself.
	return &RedisV9{connector: conn, client: client, closed: closedFlagFor(client)}, nil
}
