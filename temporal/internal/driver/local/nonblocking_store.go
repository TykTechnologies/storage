package local

import "github.com/dustinxie/lockfree"

type LockFreeStore struct {
	store  lockfree.HashMap
	Broker Broker
}

func NewLockFreeStore() *LockFreeStore {
	return &LockFreeStore{
		store:  lockfree.NewHashMap(),
		Broker: NewMockBroker(),
	}
}

func (m *LockFreeStore) Get(key string) (*Object, error) {
	v, ok := m.store.Get(key)
	if !ok {
		return nil, nil
	}

	if v == nil {
		return nil, nil
	}

	return v.(*Object), nil
}

func (m *LockFreeStore) Set(key string, value interface{}) error {
	m.store.Set(key, value)
	return nil
}

func (m *LockFreeStore) Delete(key string) error {
	m.store.Del(key)
	return nil
}

func (m *LockFreeStore) FlushAll() error {
	delList := []interface{}{}
	f := func(k interface{}, v interface{}) error {
		delList = append(delList, k)
		return nil
	}

	m.store.Lock()
	for k, v, ok := m.store.Next(); ok; k, v, ok = m.store.Next() {
		if err := f(k, v); err != nil {
			// unlock the map before return, otherwise it will deadlock
			m.store.Unlock()
			return err
		}
	}
	m.store.Unlock()

	for _, k := range delList {
		m.store.Del(k)
	}

	return nil
}

func (m *LockFreeStore) Features() map[ExtendedFeature]bool {
	return map[ExtendedFeature]bool{
		FeatureFlushAll:   true,
		FeatureHardDelete: true,
	}
}
