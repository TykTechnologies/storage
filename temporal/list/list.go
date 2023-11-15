package list

import (
	connectorType "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/TykTechnologies/storage/temporal/list/internal/driver/redisv8"
	"github.com/TykTechnologies/storage/temporal/list/internal/types"
)

type List types.List

var _ types.List = (*redisv8.RedisV8List)(nil)

func NewList(connector connectorType.Connector) (List, error) {
	switch connector.Type() {
	case "redisv8":
		return redisv8.NewList(connector)
	}

	return nil, nil
}
