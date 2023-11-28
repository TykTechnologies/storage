package set

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type Set = model.Set

var _ Set = (*redisv8.RedisV8)(nil)

func NewSet(conn model.Connector) (Set, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
