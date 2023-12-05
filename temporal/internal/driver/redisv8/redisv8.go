package redisv8

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
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

	var err error
	var tlsConfig *tls.Config

	if baseConfig.TLS != nil && baseConfig.TLS.Enable {
		tlsConfig, err = handleTLS(baseConfig.TLS)
		if err != nil {
			return nil, err
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

func handleTLS(cfg *model.TLS) (*tls.Config, error) {
	TLSConf := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}

		TLSConf.Certificates = []tls.Certificate{cert}
	}

	if cfg.CAFile != "" {
		caPem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPem) {
			return nil, fmt.Errorf("failed to add CA certificate")
		}

		TLSConf.RootCAs = certPool
	}

	minVersion, maxVersion, err := handleTLSVersion(cfg)
	if err != nil {
		return nil, err
	}

	TLSConf.MinVersion = uint16(minVersion)
	TLSConf.MaxVersion = uint16(maxVersion)

	return TLSConf, nil
}

func handleTLSVersion(cfg *model.TLS) (minVersion, maxVersion int, err error) {
	validVersions := map[string]int{
		"1.0": tls.VersionTLS10,
		"1.1": tls.VersionTLS11,
		"1.2": tls.VersionTLS12,
		"1.3": tls.VersionTLS13,
	}

	if cfg.MaxVersion == "" {
		cfg.MaxVersion = "1.3"
	}

	if _, ok := validVersions[cfg.MaxVersion]; ok {
		maxVersion = validVersions[cfg.MaxVersion]
	} else {
		err = errors.New("Invalid MaxVersion specified. Please specify a valid TLS version: 1.0, 1.1, 1.2, or 1.3")
		return
	}

	if cfg.MinVersion == "" {
		cfg.MinVersion = "1.2"
	}

	if _, ok := validVersions[cfg.MinVersion]; ok {
		minVersion = validVersions[cfg.MinVersion]
	} else {
		err = errors.New("Invalid MinVersion specified. Please specify a valid TLS version: 1.0, 1.1, 1.2, or 1.3")
		return
	}

	if minVersion > maxVersion {
		err = errors.New(
			"MinVersion is higher than MaxVersion. Please specify a valid MinVersion that is lower or equal to MaxVersion",
		)

		return
	}

	return
}
