package connector

import (
	"github.com/TykTechnologies/storage/temporal/connector/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/types"
)

var WithRedisConfig = types.WithRedisConfig

var _ types.Connector = (*redisv8.Connector)(nil)

// NewConnector returns a new connector based on the type. You have to specify the connector Configuration as an Option.
func NewConnector(connType string, options ...types.Option) (types.Connector, error) {
	switch connType {
	case types.RedisV8Type:
		return redisv8.NewConnector(options...)
	default:
		return nil, types.ErrInvalidHandlerType
	}
}
