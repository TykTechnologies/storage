package redisv8

import (
	connectorTypes "github.com/TykTechnologies/storage/temporal/connector/types"
	keyValueTypes "github.com/TykTechnologies/storage/temporal/keyvalue/types"

	"github.com/go-redis/redis/v8"
)

type KeyValueRedisV8 struct {
	client redis.UniversalClient
	connectorTypes.Connector
}

func NewKeyValueRedisV8(conn connectorTypes.Connector) (*KeyValueRedisV8, error) {
	var client redis.UniversalClient
	if ok := conn.As(&client); !ok {
		return nil, keyValueTypes.ErrInvalidConnector
	}

	return &KeyValueRedisV8{client, conn}, nil
}
