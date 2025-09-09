package ratelimiter

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type RateLimit = model.RateLimit

var _ RateLimit = (*redisv8.RedisV8)(nil)

func NewRateLimit(conn model.Connector) (RateLimit, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
