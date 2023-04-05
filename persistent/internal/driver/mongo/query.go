package mongo

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/TykTechnologies/storage/persistent/dbm"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/TykTechnologies/storage/persistent/id"
)

func buildLimitQuery(fields ...string) bson.D {
	order := bson.D{}

	for _, field := range fields {
		if field == "" {
			continue
		}

		n := 1
		var kind string

		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
				}
			}

			switch field[0] {
			case '+':
				field = field[1:]
			case '-':
				n = -1
				field = field[1:]
			}
		}

		if kind == "textScore" {
			order = append(order, primitive.E{Key: field, Value: bson.M{"$meta": kind}})
		} else {
			order = append(order, primitive.E{Key: field, Value: n})
		}
	}

	return order
}

func handleQueryValue(key string, value interface{}, search bson.M) {
	switch {
	case isNestedQuery(value):
		handleNestedQuery(search, key, value)
	case reflect.ValueOf(value).Kind() == reflect.Slice && key != "$or":
		strSlice, isStrSlice := value.([]string)

		if isStrSlice && key == "_id" {
			objectIDs := []id.ObjectId{}
			for _, str := range strSlice {
				objectIDs = append(objectIDs, id.ObjectIdHex(str))
			}

			search[key] = bson.M{"$in": objectIDs}

			return
		}

		search[key] = primitive.M{"$in": value}
	default:
		search[key] = value
	}
}

// isNestedQuery returns true if the value is dbm.DBM
func isNestedQuery(value interface{}) bool {
	_, ok := value.(dbm.DBM)
	return ok
}

// handleNestedQuery replace children queries by it nested values.
// For example, transforms a dbm.DBM{"testName": dbm.DBM{"$ne": "123"}} to {"testName":{"$ne":"123"}}
func handleNestedQuery(search bson.M, key string, value interface{}) {
	nestedQuery, ok := value.(dbm.DBM)
	if !ok {
		return
	}

	for nestedKey, nestedValue := range nestedQuery {
		switch nestedKey {
		case "$i":
			if stringValue, ok := nestedValue.(string); ok {
				quoted := regexp.QuoteMeta(stringValue)
				search[key] = &primitive.Regex{Pattern: fmt.Sprintf("^%s$", quoted), Options: "i"}
			}
		case "$text":
			if stringValue, ok := nestedValue.(string); ok {
				search[key] = bson.M{"$regex": primitive.Regex{Pattern: regexp.QuoteMeta(stringValue), Options: "i"}}
			}
		default:
			search[key] = bson.M{nestedKey: nestedValue}
		}
	}
}

// buildQuery transforms dbm.DBM into bson.M (primitive.M) it does some special treatment to nestedQueries
// using handleNestedQuery func.
func buildQuery(query dbm.DBM) bson.M {
	search := bson.M{}

	for key, value := range query {
		switch key {
		case "_sort", "_collection", "_limit", "_offset", "_date_sharding":
			continue
		case "_id":
			if id, ok := value.(id.ObjectId); ok {
				search[key] = id
				continue
			}

			handleQueryValue(key, value, search)
		default:
			handleQueryValue(key, value, search)
		}
	}

	return search
}
