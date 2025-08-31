//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestObjectToMap(t *testing.T) {
	// Test case 1: Convert a simple struct to map
	t.Run("ConvertSimpleStruct", func(t *testing.T) {
		// Create a simple test struct
		type SimpleStruct struct {
			ID        int    `json:"id"`
			Name      string `json:"name"`
			IsActive  bool   `json:"is_active"`
			CreatedAt time.Time
		}

		now := time.Now()
		simpleObj := SimpleStruct{
			ID:        123,
			Name:      "Test Object",
			IsActive:  true,
			CreatedAt: now,
		}

		// Convert to map
		result, err := objectToMap(simpleObj)
		assert.NoError(t, err, "Converting simple struct to map should not return an error")

		// Check that the map contains the expected fields
		assert.Equal(t, 4, len(result), "Map should have 4 entries")
		assert.Equal(t, float64(123), result["id"], "ID field should be correctly mapped")
		assert.Equal(t, "Test Object", result["name"], "Name field should be correctly mapped")
		assert.Equal(t, true, result["is_active"], "IsActive field should be correctly mapped")
	})

	// Test case 2: Convert a struct with nested struct
	t.Run("ConvertStructWithNestedStruct", func(t *testing.T) {
		// Create a nested struct
		type Address struct {
			Street  string `json:"street"`
			City    string `json:"city"`
			Country string `json:"country"`
		}

		type Person struct {
			ID      int     `json:"id"`
			Name    string  `json:"name"`
			Address Address `json:"address"`
		}

		person := Person{
			ID:   456,
			Name: "John Doe",
			Address: Address{
				Street:  "123 Main St",
				City:    "New York",
				Country: "USA",
			},
		}

		// Convert to map
		result, err := objectToMap(person)
		assert.NoError(t, err, "Converting struct with nested struct to map should not return an error")

		// Check that the map contains the expected fields
		assert.Equal(t, 3, len(result), "Map should have 3 entries")
		assert.Equal(t, float64(456), result["id"], "ID field should be correctly mapped")
		assert.Equal(t, "John Doe", result["name"], "Name field should be correctly mapped")

		// Check nested struct
		addressMap, ok := result["address"].(map[string]interface{})
		assert.True(t, ok, "Address should be converted to a map")
		assert.Equal(t, "123 Main St", addressMap["street"], "Street field should be correctly mapped")
		assert.Equal(t, "New York", addressMap["city"], "City field should be correctly mapped")
		assert.Equal(t, "USA", addressMap["country"], "Country field should be correctly mapped")
	})

	// Test case 3: Convert a struct with pointer fields
	t.Run("ConvertStructWithPointers", func(t *testing.T) {
		// Create a struct with pointer fields
		type PointerStruct struct {
			ID       int     `json:"id"`
			Name     *string `json:"name"`
			Age      *int    `json:"age"`
			IsActive *bool   `json:"is_active"`
		}

		name := "Test Pointer"
		age := 30
		active := true

		pointerObj := PointerStruct{
			ID:       789,
			Name:     &name,
			Age:      &age,
			IsActive: &active,
		}

		// Convert to map
		result, err := objectToMap(pointerObj)
		assert.NoError(t, err, "Converting struct with pointers to map should not return an error")

		// Check that the map contains the expected fields
		assert.Equal(t, 4, len(result), "Map should have 4 entries")
		assert.Equal(t, float64(789), result["id"], "ID field should be correctly mapped")
		assert.Equal(t, "Test Pointer", result["name"], "Name field should be correctly mapped")
		assert.Equal(t, float64(30), result["age"], "Age field should be correctly mapped")
		assert.Equal(t, true, result["is_active"], "IsActive field should be correctly mapped")
	})

	// Test case 4: Convert a struct with nil pointer fields
	t.Run("ConvertStructWithNilPointers", func(t *testing.T) {
		// Create a struct with nil pointer fields
		type NilPointerStruct struct {
			ID       int     `json:"id"`
			Name     *string `json:"name"`
			Age      *int    `json:"age"`
			IsActive *bool   `json:"is_active"`
		}

		nilPointerObj := NilPointerStruct{
			ID:       101,
			Name:     nil,
			Age:      nil,
			IsActive: nil,
		}

		// Convert to map
		result, err := objectToMap(nilPointerObj)
		assert.NoError(t, err, "Converting struct with nil pointers to map should not return an error")

		// Check that the map contains the expected fields
		assert.Equal(t, 4, len(result), "Map should have 4 entries")
		assert.Equal(t, float64(101), result["id"], "ID field should be correctly mapped")
		assert.Nil(t, result["name"], "Name field should be nil")
		assert.Nil(t, result["age"], "Age field should be nil")
		assert.Nil(t, result["is_active"], "IsActive field should be nil")
	})

	// Test case 5: Convert a non-struct value
	t.Run("ConvertNonStructValue", func(t *testing.T) {
		// Try to convert a non-struct value
		result, err := objectToMap(123)
		assert.Error(t, err, "Converting a non-struct value should return an error")
		assert.Empty(t, result, "Result should be empty for non-struct value")

		result, err = objectToMap("string value")
		assert.Error(t, err, "Converting a string should return an error")
		assert.Empty(t, result, "Result should be empty for string value")

		result, err = objectToMap([]int{1, 2, 3})
		assert.Error(t, err, "Converting a slice should return an error")
		assert.Empty(t, result, "Result should be empty for slice value")
	})

	// Test case 6: Convert a nil value
	t.Run("ConvertNilValue", func(t *testing.T) {
		result, _ := objectToMap(nil)
		assert.Empty(t, result, "Result should be empty for nil value")
	})
}

func TestGetCollectionName(t *testing.T) {
	// Test case 1: Object with TableName method
	t.Run("ObjectWithTableName", func(t *testing.T) {
		// Assuming TestObject has a TableNameValue field that's used in its TableName method
		obj := &TestObject{TableNameValue: "custom_table_name"}

		name, ok := getCollectionName(obj)
		if !ok {
			t.Error("Expected to get collection name, but got ok=false")
		}
		if name != "custom_table_name" {
			t.Errorf("Expected collection name 'custom_table_name', got '%s'", name)
		}
	})

	// Test case 2: Object without any collection name information
	t.Run("ObjectWithoutCollectionInfo", func(t *testing.T) {
		// Create an object that doesn't have any collection name information
		// This might be a different type than TestObject if TestObject always has a TableName

		// Example with a simple struct:
		type SimpleStruct struct {
			ID   int
			Name string
		}

		obj := SimpleStruct{ID: 5, Name: "Test"}

		name, ok := getCollectionName(obj)
		if ok {
			t.Error("Expected not to get collection name, but got ok=true")
		}
		if name != "" {
			t.Errorf("Expected empty collection name, got '%s'", name)
		}
	})

	// Test case 3: Non-struct value
	t.Run("NonStructValue", func(t *testing.T) {
		testCases := []interface{}{
			123,
			"string value",
			[]int{1, 2, 3},
			map[string]int{"key": 42},
			true,
			nil,
		}

		for _, tc := range testCases {
			name, ok := getCollectionName(tc)
			if ok {
				t.Errorf("Expected not to get collection name for %T, but got ok=true", tc)
			}
			if name != "" {
				t.Errorf("Expected empty collection name for %T, got '%s'", tc, name)
			}
		}
	})

	// Test case 4: Pointer to object
	t.Run("PointerToObject", func(t *testing.T) {
		// Assuming TestObject methods work with pointer receiver
		obj := &TestObject{TableNameValue: "pointer_table_name"}

		name, ok := getCollectionName(obj)
		if !ok {
			t.Error("Expected to get collection name from pointer, but got ok=false")
		}
		if name != "pointer_table_name" {
			t.Errorf("Expected collection name 'pointer_table_name', got '%s'", name)
		}
	})
}

type TestObject struct {
	ID             model.ObjectID `json:"id" gorm:"primaryKey"`
	Name           string         `json:"name"`
	Value          int            `json:"value"`
	CreatedAt      time.Time      `json:"created_at"`
	TableNameValue string         `json:"-"`
}

func (t *TestObject) TableName() string {
	if t.TableNameValue != "" {
		return t.TableNameValue
	}
	return "test_objects"
}

func (t *TestObject) GetObjectID() model.ObjectID {
	return t.ID
}

func (t *TestObject) SetObjectID(id model.ObjectID) {
	t.ID = id
}
