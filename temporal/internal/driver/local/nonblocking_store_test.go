package local

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestNewLockFreeStore(t *testing.T) {
	store := NewLockFreeStore()
	if store == nil {
		t.Fatal("NewLockFreeStore() returned nil")
	}
	if store.store == nil {
		t.Error("NewLockFreeStore() did not initialize store")
	}
	if store.Broker == nil {
		t.Error("NewLockFreeStore() did not initialize Broker")
	}
}

func TestLockFreeStore_Get(t *testing.T) {
	store := NewLockFreeStore()
	obj := &Object{} // Assume Object is defined elsewhere
	store.store.Set("testKey", obj)

	tests := []struct {
		name    string
		key     string
		want    *Object
		wantErr bool
	}{
		{"Existing key", "testKey", obj, false},
		{"Non-existing key", "nonExistingKey", nil, false},
		{"Nil value", "nilKey", nil, false},
	}

	store.store.Set("nilKey", nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LockFreeStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LockFreeStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLockFreeStore_Set(t *testing.T) {
	store := NewLockFreeStore()
	obj := &Object{}

	if err := store.Set("testKey", obj); err != nil {
		t.Errorf("LockFreeStore.Set() error = %v", err)
	}

	if v, ok := store.store.Get("testKey"); !ok || v != obj {
		t.Errorf("LockFreeStore.Set() did not store the object correctly")
	}
}

func TestLockFreeStore_Delete(t *testing.T) {
	store := NewLockFreeStore()
	store.store.Set("testKey", &Object{})

	if err := store.Delete("testKey"); err != nil {
		t.Errorf("LockFreeStore.Delete() error = %v", err)
	}

	if _, ok := store.store.Get("testKey"); ok {
		t.Errorf("LockFreeStore.Delete() did not remove the key")
	}
}

func TestLockFreeStore_FlushAll(t *testing.T) {
	store := NewLockFreeStore()

	// Add some test data
	testKeys := []string{"testKey1", "testKey2", "testKey3"}
	for _, key := range testKeys {
		store.Set(key, &Object{})
	}

	// Verify data is present
	for _, key := range testKeys {
		if _, ok := store.store.Get(key); !ok {
			t.Errorf("Test setup failed: key %s not found before FlushAll", key)
		}
	}

	// Perform FlushAll
	if err := store.FlushAll(); err != nil {
		t.Errorf("LockFreeStore.FlushAll() error = %v", err)
	}

	// Verify all data has been removed
	for _, key := range testKeys {
		if value, ok := store.store.Get(key); ok {
			t.Errorf("LockFreeStore.FlushAll() did not remove key %s, value: %v", key, value)
		}
	}

	// Additional check: try to add and retrieve a new key-value pair
	newKey := "newTestKey"
	newValue := &Object{}
	store.Set(newKey, newValue)

	retrievedValue, err := store.Get(newKey)
	if err != nil {
		t.Errorf("Error retrieving new key after FlushAll: %v", err)
	}
	if retrievedValue != newValue {
		t.Errorf("Retrieved value does not match set value after FlushAll")
	}
}

func TestLockFreeStore_Features(t *testing.T) {
	store := NewLockFreeStore()
	features := store.Features()

	if !features[FeatureFlushAll] {
		t.Errorf("LockFreeStore.Features() FeatureFlushAll should be true")
	}
}

func TestLockFreeStore_Concurrency(t *testing.T) {
	store := NewLockFreeStore()
	const goroutines = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines * 4) // 4 operations per goroutine

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			store.Set(key, &Object{})
		}(i)

		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			store.Get(key)
		}(i)

		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			store.Delete(key)
		}(i)

		go func() {
			defer wg.Done()
			store.FlushAll()
		}()
	}

	wg.Wait()
	// If we reach here without deadlocks or race conditions, the test passes
}
