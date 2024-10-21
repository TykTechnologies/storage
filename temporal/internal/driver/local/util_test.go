package local

import (
	"testing"
)

func TestAPI_addToKeyIndex(t *testing.T) {
	tests := []struct {
		name        string
		initialData map[string]*Object
		key         string
		wantErr     bool
	}{
		{
			name:        "New key index",
			initialData: map[string]*Object{},
			key:         "testKey",
			wantErr:     false,
		},
		{
			name: "Existing key index",
			initialData: map[string]*Object{
				keyIndexKey: {
					Type:  TypeSet,
					Value: map[string]interface{}{"existingKey": true},
					NoExp: true,
				},
			},
			key:     "newKey",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := NewMockStore()
			for k, v := range tt.initialData {
				mockStore.Set(k, v)
			}

			api := &API{Store: mockStore}
			err := api.addToKeyIndex(tt.key)

			if (err != nil) != tt.wantErr {
				t.Errorf("API.addToKeyIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify the key was added
			obj, _ := mockStore.Get(keyIndexKey)
			if obj == nil {
				t.Errorf("Key index object not found")
				return
			}

			keySet := obj.Value.(map[string]interface{})
			if _, exists := keySet[tt.key]; !exists {
				t.Errorf("Key %s not found in key index", tt.key)
			}
		})
	}
}

func TestAPI_updateDeletedKeysIndex(t *testing.T) {
	tests := []struct {
		name        string
		initialData map[string]*Object
		key         string
		wantErr     bool
	}{
		{
			name:        "New deleted key index",
			initialData: map[string]*Object{},
			key:         "deletedKey",
			wantErr:     false,
		},
		{
			name: "Existing deleted key index",
			initialData: map[string]*Object{
				deletedKeyIndexKey: {
					Type:  TypeSet,
					Value: map[string]interface{}{"existingDeletedKey": true},
					NoExp: true,
				},
			},
			key:     "newDeletedKey",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := NewMockStore()
			for k, v := range tt.initialData {
				mockStore.Set(k, v)
			}

			api := &API{Store: mockStore}
			err := api.updateDeletedKeysIndex(tt.key)

			if (err != nil) != tt.wantErr {
				t.Errorf("API.updateDeletedKeysIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify the key was added to the deleted keys index
			obj, _ := mockStore.Get(deletedKeyIndexKey)
			if obj == nil {
				t.Errorf("Deleted key index object not found")
				return
			}

			deletedKeySet := obj.Value.(map[string]interface{})
			if _, exists := deletedKeySet[tt.key]; !exists {
				t.Errorf("Key %s not found in deleted key index", tt.key)
			}
		})
	}
}
