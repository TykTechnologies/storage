package list

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
)

type List = model.List

var _ List = (*redisv8.RedisV8)(nil)

func NewList(conn model.Connector) (List, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, model.ErrInvalidHandlerType
	}
}
