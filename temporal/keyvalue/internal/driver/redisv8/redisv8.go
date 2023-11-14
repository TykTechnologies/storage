package redisv8

import (
	"github.com/TykTechnologies/storage/temporal/types"

	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	client redis.UniversalClient
	types.Connector
}

func NewRedisV8(conn types.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if ok := conn.As(&client); !ok {
		return nil, types.ErrInvalidConnector
	}

	return &RedisV8{client, conn}, nil
}
