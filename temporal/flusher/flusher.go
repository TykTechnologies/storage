package flusher

import (
	"github.com/TykTechnologies/storage/temporal/internal/driver/local"
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv9"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type Flusher = model.Flusher

func NewFlusher(conn model.Connector) (Flusher, error) {
	switch conn.Type() {
	case model.RedisV9Type:
		return redisv9.NewRedisV9WithConnection(conn)
	case model.LocalType:
		return local.NewLocalStore(conn), nil
	case model.CRDTType:
		return local.NewLocalStoreWithCRDTBackend(conn)
	default:
		return nil, temperr.InvalidHandlerType
	}
}
