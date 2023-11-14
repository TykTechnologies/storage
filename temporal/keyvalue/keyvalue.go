package temporal

import (
	"errors"

	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/types"
)

func NewKeyValue(conn types.Connector) (types.KeyValue, error) {
	switch conn.Type() {
	case types.RedisV8Type:
		return redisv8.NewRedisV8(conn)
	default:
		return nil, errors.New("invalid driver")
	}
}
