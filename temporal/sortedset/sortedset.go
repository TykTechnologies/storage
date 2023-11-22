package sortedset

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
)

type SortedSet = model.SortedSet

var _ SortedSet = (*redisv8.RedisV8)(nil)

func NewSortedSet(conn model.Connector) (SortedSet, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, model.ErrInvalidHandlerType
	}
}
