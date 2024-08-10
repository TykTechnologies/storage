package local

import (
	"sync"
)

// MockStore implements the KVStore interface using a map
type MockStore struct {
	data  map[string]interface{}
	mutex sync.RWMutex
}

func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string]interface{}),
	}
}

func (m *MockStore) Get(key string) (*Object, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if obj, ok := m.data[key]; ok {
		return obj.(*Object), nil
	}
	return nil, nil
}

func (m *MockStore) Set(key string, value interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.data[key] = value
	return nil
}

func (m *MockStore) Delete(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MockStore) FlushAll() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.data = make(map[string]interface{})
	return nil
}
