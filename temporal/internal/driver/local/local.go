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
		Value: map[string]bool{},
		NoExp: true,
	})

	api.Store.Set(deletedKeyIndexKey, &Object{
		Type:  TypeList,
		Value: map[string]bool{},
		NoExp: true,
	})

	return api
}

func (api *API) addToKeyIndex(key string) error {
	o, err := api.Store.Get(keyIndexKey)
	if err != nil {
		// not found, create new
		o = &Object{
			Type:  TypeSet,
			Value: map[string]bool{key: true},
			NoExp: true,
		}

		return api.Store.Set(keyIndexKey, o)
	}

	if o == nil {
		o = &Object{
			Type:  TypeSet,
			Value: map[string]bool{key: true},
			NoExp: true,
		}
	}

	list := o.Value.(map[string]bool)
	list[key] = true

	o.Value = list

	return api.Store.Set(keyIndexKey, o)
}

func (api *API) updateDeletedKeysIndex(key string) error {
	o, err := api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		// not found, create new
		o = &Object{
			Type:  TypeSet,
			Value: map[string]bool{key: true},
			NoExp: true,
		}

		return api.Store.Set(deletedKeyIndexKey, o)
	}

	if o == nil {
		o = &Object{
			Type:  TypeSet,
			Value: map[string]bool{key: true},
			NoExp: true,
		}
	}

	list := o.Value.(map[string]bool)
	list[key] = true
	o.Value = list

	return api.Store.Set(deletedKeyIndexKey, o)
}
