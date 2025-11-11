package postgres

import (
	"encoding/json"
	"reflect"
	"strings"
)

// objectToMap receives an interface and returns as map
func objectToMap(obj interface{}) (map[string]interface{}, error) {
	if obj == nil {
		return nil, nil
	}

	val := reflect.ValueOf(obj)

	// Dereference pointers
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}

	// Only structs can be converted to maps
	if val.Kind() != reflect.Struct {
		// Return error for unsupported types
		return nil, &json.UnsupportedTypeError{Type: val.Type()}
	}

	result := make(map[string]interface{})
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Get field name from json tag, fallback to field name
		fieldName := fieldType.Name

		tag := fieldType.Tag.Get("json")
		if tag == "-" {
			// Skip fields with json:"-"
			continue
		}

		if tag != "" {
			// Handle json tag options (e.g., "name,omitempty")
			if commaIdx := strings.IndexByte(tag, ','); commaIdx > 0 {
				fieldName = tag[:commaIdx]
			} else {
				fieldName = tag
			}
		}

		// Convert field value
		fieldValue, err := convertValue(field)
		if err != nil {
			return nil, err
		}

		result[fieldName] = fieldValue
	}

	return result, nil
}

// convertValue converts a reflect.Value to an interface{} suitable for map storage
func convertValue(val reflect.Value) (interface{}, error) {
	// Handle nil pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}

	// Handle special types that need JSON marshaling
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		// These types cannot be marshaled
		return nil, &json.UnsupportedTypeError{Type: val.Type()}

	case reflect.Struct:
		// Handle time.Time as a special case - return it directly
		if val.Type().String() == "time.Time" {
			return val.Interface(), nil
		}

		// Recursively convert nested structs
		return objectToMap(val.Interface())

	case reflect.Slice, reflect.Array:
		// Convert slices/arrays element by element
		length := val.Len()
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			elem, err := convertValue(val.Index(i))
			if err != nil {
				return nil, err
			}
			result[i] = elem
		}
		return result, nil

	case reflect.Map:
		// Convert maps key by key
		result := make(map[string]interface{})
		iter := val.MapRange()
		for iter.Next() {
			key := iter.Key()
			// Only support string keys for simplicity
			if key.Kind() != reflect.String {
				// Fall back to interface for complex keys
				return val.Interface(), nil
			}
			elem, err := convertValue(iter.Value())
			if err != nil {
				return nil, err
			}
			result[key.String()] = elem
		}
		return result, nil

	default:
		// For basic types, just return the interface value
		return val.Interface(), nil
	}
}

// Helper function to get collection name from a struct type
func getCollectionName(result interface{}) (string, bool) {
	if result == nil {
		return "", false
	}

	val := reflect.ValueOf(result)

	// unwrap pointer(s)
	for val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	// helper: try method by name
	tryMethod := func(v reflect.Value, name string) (string, bool) {
		m := v.MethodByName(name)
		if !m.IsValid() || m.Type().NumIn() != 0 || m.Type().NumOut() != 1 || m.Type().Out(0).Kind() != reflect.String {
			return "", false
		}

		return m.Call(nil)[0].String(), true
	}

	// try methods in order
	for _, method := range []string{"TableName", "CollectionName", "GetCollection"} {
		if name, ok := tryMethod(reflect.ValueOf(result), method); ok {
			return name, true
		}

		if name, ok := tryMethod(val, method); ok {
			return name, true
		}
	}

	// check for "Collection" field in struct
	if val.Kind() == reflect.Struct {
		if f := val.FieldByName("Collection"); f.IsValid() && f.Kind() == reflect.String {
			return f.String(), true
		}
	}

	return "", false
}
