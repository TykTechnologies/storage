package temporal

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type KeyValue = model.KeyValue

var _ KeyValue = (*redisv8.RedisV8)(nil)

// NewKeyValue returns a new model.KeyValue storage based on the type of the connector.
func NewKeyValue(conn model.Connector) (KeyValue, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
