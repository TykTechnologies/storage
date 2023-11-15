package temporal

import (
	"errors"

	connectorTypes "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/driver/redisv8"
	keyValueTypes "github.com/TykTechnologies/storage/temporal/keyvalue/types"
)

func NewKeyValue(conn connectorTypes.Connector) (keyValueTypes.KeyValue, error) {
	switch conn.Type() {
	case connectorTypes.RedisV8Type:
		return redisv8.NewKeyValueRedisV8(conn)
	default:
		return nil, errors.New("invalid driver")
	}
}
