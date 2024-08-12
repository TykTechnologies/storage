package local

import (
	"github.com/TykTechnologies/storage/temporal/model"
)

type API struct {
	// Store has no inherent features except storing and retrieving data
	Store     KVStore
	Connector model.Connector
	Broker    Broker
}

const (
	keyIndexKey        = "rumbaba:keyIndex"
	deletedKeyIndexKey = "rumbaba:deletedKeyIndex"

	TypeBytes     = "bytes"
	TypeSet       = "set"
	TypeSortedSet = "sortedset"
	TypeList      = "list"
	TypeCounter   = "counter"
)

var mockStore *MockStore

func init() {
	mockStore = NewMockStore()
}

func GetMockStore() *MockStore {
	return mockStore
}

func NewLocalConnector(withStore KVStore) model.Connector {
	return &LocalConnector{
		Store:     withStore,
		connected: true,
		Broker:    NewMockBroker(),
	}
}

func NewLocalStore(connector model.Connector) *API {
	api := &API{
		Connector: connector,
		Store:     connector.(*LocalConnector).Store,
		Broker:    connector.(*LocalConnector).Broker,
	}

	// init the key indexes
	api.Store.Set(keyIndexKey, &Object{
		Type:  TypeList,
		Value: map[string]interface{}{},
		NoExp: true,
	})

	api.Store.Set(deletedKeyIndexKey, &Object{
		Type:  TypeList,
		Value: map[string]interface{}{},
		NoExp: true,
	})

	return api
}
