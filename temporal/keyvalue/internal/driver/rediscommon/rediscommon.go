package rediscommon

import (
	"crypto/tls"
	"time"

	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/types"
	"github.com/TykTechnologies/storage/temporal/keyvalue/utils"
)

type CommonRedisConfig struct {
	Addrs            []string
	MasterName       string
	SentinelPassword string
	Username         string
	Password         string
	DB               int
	DialTimeout      time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	PoolSize         int
	TLSConfig        *tls.Config
}

func NewCommonRedisConfig(opts *types.ClientOpts) *CommonRedisConfig {
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

	return &CommonRedisConfig{
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
}
