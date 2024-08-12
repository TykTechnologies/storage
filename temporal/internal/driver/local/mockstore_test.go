package local

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestNewMockStore(t *testing.T) {
	store := NewMockStore()
	if store == nil {
		t.Fatal("NewMockStore() returned nil")
	}
	if store.data == nil {
		t.Error("NewMockStore() did not initialize data map")
	}
}

func TestMockStore_Get(t *testing.T) {
	store := NewMockStore()
	obj := &Object{} // Assume Object is defined elsewhere
	store.data["testKey"] = obj

	tests := []struct {
		name    string
		key     string
		want    *Object
		wantErr bool
	}{
		{"Existing key", "testKey", obj, false},
		{"Non-existing key", "nonExistingKey", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("MockStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MockStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMockStore_Set(t *testing.T) {
	store := NewMockStore()
	obj := &Object{}

	if err := store.Set("testKey", obj); err != nil {
		t.Errorf("MockStore.Set() error = %v", err)
	}

	if store.data["testKey"] != obj {
		t.Errorf("MockStore.Set() did not store the object correctly")
	}
}

func TestMockStore_Delete(t *testing.T) {
	store := NewMockStore()
	store.data["testKey"] = &Object{}

	if err := store.Delete("testKey"); err != nil {
		t.Errorf("MockStore.Delete() error = %v", err)
	}

	if _, exists := store.data["testKey"]; exists {
		t.Errorf("MockStore.Delete() did not remove the key")
	}
}

func TestMockStore_FlushAll(t *testing.T) {
	store := NewMockStore()
	store.data["testKey"] = &Object{}

	if err := store.FlushAll(); err != nil {
		t.Errorf("MockStore.FlushAll() error = %v", err)
	}

	if len(store.data) != 0 {
		t.Errorf("MockStore.FlushAll() did not remove all keys")
	}
}

func TestMockStore_Features(t *testing.T) {
	store := NewMockStore()
	features := store.Features()

	if !features[FeatureFlushAll] {
		t.Errorf("MockStore.Features() FeatureFlushAll should be true")
	}
}

func TestMockStore_Concurrency(t *testing.T) {
	store := NewMockStore()
	const goroutines = 100
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
