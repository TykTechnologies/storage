package temporal

import (
	"errors"

	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/driver/redisv9"
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/types"
)

const (
	RedisV8 string = "redis-v8"
	RedisV9 string = "redis-v9"
)

func NewKeyValue(opts *types.ClientOpts) (types.KeyValue, error) {
	switch opts.Type {
	case RedisV8:
		if opts.Redis == nil {
			return nil, errors.New("redis client options not provided for redis-8 driver")
		}

		return redisv8.NewRedisV8(opts), nil
	case RedisV9:
		if opts.Redis == nil {
			return nil, errors.New("redis client options not provided for redis-9 driver")
		}

		return redisv9.NewRedisV9(opts), nil
	default:
		return nil, errors.New("invalid driver")
	}
}
