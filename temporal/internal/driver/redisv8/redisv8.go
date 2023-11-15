package redisv8

import (
	connectorType "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/go-redis/redis/v8"
)

type RedisV8 struct {
	connector connectorType.Connector
	client    redis.UniversalClient
}

// NewList returns a new redisv8List instance.
func NewRedisV8(conn connectorType.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if conn == nil || !conn.As(&client) {
		return nil, types.ErrInvalidConnector
	}

	return &RedisV8{connector: conn, client: client}, nil
}

func NewRedisV8WithConnection(conn connectorType.Connector) (*RedisV8, error) {
	var client redis.UniversalClient
	if conn == nil || !conn.As(&client) {
		return nil, types.ErrInvalidConnector
	}

	return &RedisV8{connector: conn, client: client}, nil
}
