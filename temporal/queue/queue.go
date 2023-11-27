package queue

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type Queue = model.Queue

var _ Queue = (*redisv8.RedisV8)(nil)

func NewQueue(conn model.Connector) (Queue, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
