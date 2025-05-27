package postgres

import (
	"encoding/json"
	"reflect"
)

// Helper functions
func objectToMap(obj interface{}) (map[string]interface{}, error) {
	// Convert object to JSON and then to map
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	return result, err
}

// Helper function to get collection name from a struct type
func getCollectionName(result interface{}) (string, bool) {
	if result == nil {
		return "", false
	}

	t := reflect.TypeOf(result)

	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Handle slice types
	if t.Kind() == reflect.Slice {
		t = t.Elem()

		// If it's a slice of pointers, get the element type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}

	// Now t should be a struct type
	if t.Kind() != reflect.Struct {
		return "", false
	}

	// Try to find a TableName method
	method, ok := t.MethodByName("TableName")
	if ok {
		// Call the TableName method
		tableName := method.Func.Call([]reflect.Value{reflect.New(t)})[0].String()
		return tableName, true
	}

	// If no TableName method, use the struct name
	return t.Name(), true
}
