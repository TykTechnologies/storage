package local

import (
	"context"
	"sync"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

type LocalConnector struct {
	Store     KVStore
	Broker    Broker
	mutex     sync.RWMutex
	connected bool
}

// Disconnect disconnects from the backend
func (api *LocalConnector) Disconnect(context.Context) error {
	api.mutex.RLock()
	defer api.mutex.RUnlock()
	api.connected = false
	return nil
}

// Ping executes a ping to the backend
func (api *LocalConnector) Ping(context.Context) error {
	if !api.connected {
		return temperr.ClosedConnection
	}

	return nil
}

// Type returns the  connector type
func (api *LocalConnector) Type() string {
	return "local"
}

// As converts i to driver-specific types.
// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
func (api *LocalConnector) As(i interface{}) bool {
	if _, ok := i.(*API); ok {
		return true
	}

	return false
}
