package connector

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

var WithRedisConfig = model.WithRedisConfig

var _ model.Connector = (*redisv8.RedisV8)(nil)

// NewConnector returns a new connector based on the type. You have to specify the connector Configuration as an Option.
func NewConnector(connType string, options ...model.Option) (model.Connector, error) {
	switch connType {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithOpts(options...)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
