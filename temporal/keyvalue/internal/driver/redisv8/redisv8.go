package redisv8

import (
	connectorTypes "github.com/TykTechnologies/storage/temporal/connector/types"
	keyValueTypes "github.com/TykTechnologies/storage/temporal/keyvalue/types"

	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	client redis.UniversalClient
	connectorTypes.Connector
}

func NewRedisV8(conn connectorTypes.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if ok := conn.As(&client); !ok {
		return nil, keyValueTypes.ErrInvalidConnector
	}

	return &RedisV8{client, conn}, nil
}
