package postgres

import (
	"encoding/json"
	"github.com/TykTechnologies/storage/persistent/model"
	"reflect"
	"strings"
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

func ensureID(originalID model.ObjectID, row model.DBObject, query model.DBM) {
	if originalID != "" {
		row.SetObjectID(originalID)
	} else if qid, ok := query["id"]; ok {
		if sid, ok2 := qid.(string); ok2 && sid != "" {
			row.SetObjectID(model.ObjectIDHex(sid))
		}
	}

	// Always ensure there's an ID
	if row.GetObjectID() == "" {
		row.SetObjectID(model.NewObjectID())
	}
}

func cloneDBObject(row model.DBObject) model.DBObject {
	newRow := reflect.New(reflect.TypeOf(row).Elem()).Interface().(model.DBObject)
	newRow.SetObjectID(row.GetObjectID())
	return newRow
}

func mergeQueryFields(row model.DBObject, query model.DBM) {
	for k, v := range query {
		if strings.HasPrefix(k, "_") || k == "$or" {
			continue
		}
		setField(row, k, v) // keeps reflection logic isolated
	}
}
