package local

import (
	"strings"
	"time"

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

func (api *API) IncrementWithExpire(key string, ttl int64) int64 {
	o, err := api.Store.Get(key)
	if err != nil {
		return 0
	}

	if o == nil {
		o = &Object{
			Value: 1,
			Type:  TypeCounter,
		}
	} else {
		if v, ok := o.Value.(int); ok {
			o.Value = v + 1
		}
	}

	o.SetExpire(time.Duration(ttl) * time.Second)
	o.NoExp = false

	api.Store.Set(key, o)

	return int64(o.Value.(int))
}

func (api *API) DeleteAllKeys() bool {
	o, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return false
	}

	if o == nil {
		return false
	}

	for key, _ := range o.Value.(map[string]bool) {
		api.Store.Delete(key)
	}

	api.Store.Delete(keyIndexKey)

	return true
}

func (api *API) AddToList(keyName string, value string) {
	o, err := api.Store.Get(keyName)
	if err != nil {
		return
	}

	if o == nil {
		o = &Object{
			Type:  TypeList,
			Value: []interface{}{value},
		}

		api.Store.Set(keyName, o)
		return
	}

	if o.Type != TypeList {
		return
	}

	o.Value = append(o.Value.([]interface{}), value)

	api.Store.Set(keyName, o)
}

func (a *API) RemoveFromList(keyName, value string) {
	o, err := a.Store.Get(keyName)
	if err != nil {
		return
	}

	if o == nil {
		return
	}

	if o.Type != TypeList {
		return
	}

	var newList []interface{}
	for _, v := range o.Value.([]interface{}) {
		if v == value {
			continue
		}

		newList = append(newList, v)
	}

	o.Value = newList

	a.Store.Set(keyName, o)
}

func (api *API) GetListRange(keyName string, from, to int64) ([]interface{}, error) {
	o, err := api.Store.Get(keyName)
	if err != nil {
		return nil, err
	}

	if o == nil || o.Type != TypeList {
		return nil, nil
	}

	list := o.Value.([]interface{})
	length := int64(len(list))

	// Convert negative indices to positive
	if from < 0 {
		if from < 0 {
			from = 0
		}
	}
	if to < 0 {
		to = length
	}

	// Ensure from is not greater than length
	if from >= length {
		return []interface{}{}, nil
	}

	// Ensure to is not greater than length
	if to >= length {
		to = length - 1
	}

	// Ensure from is not greater than to
	if from > to {
		return []interface{}{}, nil
	}

	// +1 because slicing in Go is exclusive for the upper bound
	return list[from : to+1], nil
}

func (api *API) ScanKeys(pattern string) ([]string, error) {
	// Strip the trailing "*" from the pattern
	pattern = strings.TrimSuffix(pattern, "*")

	// Get the key index
	keyIndexObj, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return nil, err
	}
	if keyIndexObj == nil {
		return nil, nil
	}
	keyIndex := keyIndexObj.Value.(map[string]bool)

	// Get the deleted key index
	deletedKeyIndexObj, err := api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		return nil, err
	}
	var deletedKeys map[string]bool
	if deletedKeyIndexObj != nil {
		deletedKeysList := deletedKeyIndexObj.Value.(map[string]bool)
		deletedKeys = make(map[string]bool, len(deletedKeysList))
		for key, _ := range deletedKeysList {
			deletedKeys[key] = true
		}
	} else {
		deletedKeys = make(map[string]bool)
	}

	var keys []string
	for key, _ := range keyIndex {
		// Check if the key matches the pattern and is not deleted
		if (pattern == "" || strings.HasPrefix(key, pattern)) && !deletedKeys[key] {
			keys = append(keys, key)
		}
	}

	return keys, nil
}
