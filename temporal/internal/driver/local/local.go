package local

import (
	"github.com/TykTechnologies/storage/temporal/model"
)

type API struct {
	// Store has no inherent features except storing and retrieving data
	Store     KVStore
	Connector model.Connector
	Broker    Broker
	Stopped   bool
}

const (
	keyIndexKey        = "localstore:keyIndex"
	deletedKeyIndexKey = "localstore:deletedKeyIndex"
	ttlIndexKey        = "localstore:ttlIndex"

	TypeBytes     = "bytes"
	TypeSet       = "set"
	TypeSortedSet = "sortedset"
	TypeList      = "list"
	TypeCounter   = "counter"
	TypeDeleted   = "deleted"
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

	initAlready, _ := api.Store.Get(keyIndexKey)
	if initAlready != nil {
		return api
	}

	// init the key indexes
	api.initialiseKeyIndexes()
	return api
}

func (api *API) initialiseKeyIndexes() {
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

	api.Store.Set(ttlIndexKey, &Object{
		Type:  TypeList,
		Value: map[string]interface{}{},
		NoExp: true,
	})

	go api.walkTTLIndex()
}

func (api *API) Stop() {
	api.Stopped = true
}
