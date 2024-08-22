package temporal

import (
	"fmt"

	"github.com/TykTechnologies/storage/temporal/internal/driver/local"
	"github.com/TykTechnologies/storage/temporal/internal/driver/redisv9"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

type KeyValue = model.KeyValue

var _ KeyValue = (*redisv9.RedisV9)(nil)

// NewKeyValue returns a new model.KeyValue storage based on the type of the connector.
func NewKeyValue(conn model.Connector) (KeyValue, error) {
	switch conn.Type() {
	case model.RedisV9Type:
		return redisv9.NewRedisV9WithConnection(conn)
	case model.LocalType:
		return local.NewLocalStore(conn), nil
	case model.CRDTType:
		return local.NewLocalStoreWithCRDTBackend(conn)

	default:
		return nil, fmt.Errorf("err: %v, type: %v", temperr.InvalidHandlerType, conn.Type())
	}
}
