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
	// Check if result is nil
	if result == nil {
		return "", false
	}

	// Get the value and type using reflection
	resultValue := reflect.ValueOf(result)
	resultType := resultValue.Type()

	// First, try to call TableName() method regardless of whether it's a pointer or value
	// This handles both value receivers and pointer receivers
	tableNameMethod := resultValue.MethodByName("TableName")
	if tableNameMethod.IsValid() && tableNameMethod.Type().NumIn() == 0 && tableNameMethod.Type().NumOut() == 1 && tableNameMethod.Type().Out(0).Kind() == reflect.String {
		returnValues := tableNameMethod.Call(nil)
		return returnValues[0].String(), true
	}

	// If the object is not a pointer but has a pointer method, try that
	if resultType.Kind() != reflect.Ptr {
		// Create a pointer to the value and check if it has TableName method
		ptrValue := reflect.New(resultType)
		ptrValue.Elem().Set(resultValue)

		tableNameMethod := ptrValue.MethodByName("TableName")
		if tableNameMethod.IsValid() && tableNameMethod.Type().NumIn() == 0 && tableNameMethod.Type().NumOut() == 1 && tableNameMethod.Type().Out(0).Kind() == reflect.String {
			returnValues := tableNameMethod.Call(nil)
			return returnValues[0].String(), true
		}
	}

	// If the object is a pointer, also check the value it points to
	if resultType.Kind() == reflect.Ptr {
		// Get the element the pointer points to
		elemValue := resultValue.Elem()
		if elemValue.IsValid() {
			// Check if the element has a TableName method
			tableNameMethod := elemValue.MethodByName("TableName")
			if tableNameMethod.IsValid() && tableNameMethod.Type().NumIn() == 0 && tableNameMethod.Type().NumOut() == 1 && tableNameMethod.Type().Out(0).Kind() == reflect.String {
				returnValues := tableNameMethod.Call(nil)
				return returnValues[0].String(), true
			}
		}
	}

	// Check for other methods that might provide a collection name
	// Try CollectionName method
	collNameMethod := resultValue.MethodByName("CollectionName")
	if collNameMethod.IsValid() && collNameMethod.Type().NumIn() == 0 && collNameMethod.Type().NumOut() == 1 && collNameMethod.Type().Out(0).Kind() == reflect.String {
		returnValues := collNameMethod.Call(nil)
		return returnValues[0].String(), true
	}

	// Try GetCollection method
	getCollMethod := resultValue.MethodByName("GetCollection")
	if getCollMethod.IsValid() && getCollMethod.Type().NumIn() == 0 && getCollMethod.Type().NumOut() == 1 && getCollMethod.Type().Out(0).Kind() == reflect.String {
		returnValues := getCollMethod.Call(nil)
		return returnValues[0].String(), true
	}

	// If we have a struct, check for a Collection field
	if resultType.Kind() == reflect.Ptr && resultValue.Elem().Kind() == reflect.Struct {
		// For pointer to struct, check the struct it points to
		elemValue := resultValue.Elem()
		collField := elemValue.FieldByName("Collection")
		if collField.IsValid() && collField.Kind() == reflect.String {
			return collField.String(), true
		}
	} else if resultType.Kind() == reflect.Struct {
		// For struct value, check directly
		collField := resultValue.FieldByName("Collection")
		if collField.IsValid() && collField.Kind() == reflect.String {
			return collField.String(), true
		}
	}

	// If we have a TableNameValue field, use that
	if resultType.Kind() == reflect.Ptr && resultValue.Elem().Kind() == reflect.Struct {
		// For pointer to struct, check the struct it points to
		elemValue := resultValue.Elem()
		tableNameField := elemValue.FieldByName("TableNameValue")
		if tableNameField.IsValid() && tableNameField.Kind() == reflect.String && tableNameField.String() != "" {
			return tableNameField.String(), true
		}
	} else if resultType.Kind() == reflect.Struct {
		// For struct value, check directly
		tableNameField := resultValue.FieldByName("TableNameValue")
		if tableNameField.IsValid() && tableNameField.Kind() == reflect.String && tableNameField.String() != "" {
			return tableNameField.String(), true
		}
	}

	// No collection name found
	return "", false
}
