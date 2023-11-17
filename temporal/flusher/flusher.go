package flusher

import (
	"errors"

	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/model"
)

type Flusher = model.Flusher

func NewFlusher(conn model.Connector) (Flusher, error) {
	switch conn.Type() {
	case model.RedisV8Type:
		return redisv8.NewRedisV8WithConnection(conn)
	default:
		return nil, errors.New("invalid driver")
	}
}
