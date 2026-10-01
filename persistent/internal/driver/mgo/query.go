package mgo

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/mgo.v2/bson"

	"github.com/TykTechnologies/storage/persistent/model"
)

// buildProjection builds an inclusion selector from field names, dropping blanks, duplicates and
// children of a listed parent so both Mongo drivers project the same paths; nil when nothing is left.
func buildProjection(fields []string) bson.M {
	projection := bson.M{}

	for _, field := range projectionFields(fields) {
		projection[field] = 1
	}

	if len(projection) == 0 {
		return nil
	}

	return projection
}

// projectionFields trims the names, keeps the first occurrence of each and skips a child path
// whose parent is also listed.
func projectionFields(fields []string) []string {
	seen := make(map[string]struct{}, len(fields))
	names := make([]string, 0, len(fields))

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		if _, ok := seen[field]; ok {
			continue
		}

		seen[field] = struct{}{}

		names = append(names, field)
	}

	kept := names[:0]

	for _, field := range names {
		if hasListedParent(field, seen) {
			continue
		}

		kept = append(kept, field)
	}

	return kept
}

func hasListedParent(field string, listed map[string]struct{}) bool {
	for i := strings.LastIndex(field, "."); i > 0; i = strings.LastIndex(field[:i], ".") {
		if _, ok := listed[field[:i]]; ok {
			return true
		}
	}

	return false
}

func buildQuery(query model.DBM) bson.M {
	search := bson.M{}

	for key, value := range query {
		switch key {
		case "_sort", "_collection", "_limit", "_offset", "_date_sharding":
			continue
		case "_id":
			if id, ok := value.(model.ObjectID); ok {
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

func handleQueryValue(key string, value interface{}, search bson.M) {
	switch {
	case isNestedQuery(value):
		handleNestedQuery(search, key, value)
	case reflect.ValueOf(value).Kind() == reflect.Slice && key != "$or":
		strSlice, isStr := value.([]string)

		if isStr && key == "_id" {
			ObjectIDs := []model.ObjectID{}
			for _, str := range strSlice {
				if bson.IsObjectIdHex(str) {
					ObjectIDs = append(ObjectIDs, model.ObjectIDHex(str))
				}
			}

			search[key] = bson.M{"$in": ObjectIDs}

			return
		}

		search[key] = bson.M{"$in": value}
	default:
		search[key] = value
	}
}

func isNestedQuery(value interface{}) bool {
	_, ok := value.(model.DBM)
	return ok
}

func handleNestedQuery(search bson.M, key string, value interface{}) {
	nestedQuery, ok := value.(model.DBM)
	if !ok {
		return
	}

	for nestedKey, nestedValue := range nestedQuery {
		switch nestedKey {
		case "$i":
			if stringValue, ok := nestedValue.(string); ok {
				quoted := regexp.QuoteMeta(stringValue)
				search[key] = &bson.RegEx{Pattern: fmt.Sprintf("^%s$", quoted), Options: "i"}
			}
		case "$text":
			if stringValue, ok := nestedValue.(string); ok {
				search[key] = bson.M{"$regex": bson.RegEx{Pattern: regexp.QuoteMeta(stringValue), Options: "i"}}
			}
		default:
			if v, ok := search[key]; !ok {
				search[key] = bson.M{nestedKey: nestedValue}
			} else {
				if nestedQ, ok := v.(bson.M); ok {
					nestedQ[nestedKey] = nestedValue
					search[key] = nestedQ
				}
			}
		}
	}
}

func getColName(query model.DBM, row model.DBObject) (string, error) {
	colName, ok := query["_collection"].(string)
	if !ok {
		if row == nil {
			return "", errors.New("unable to find collection name")
		}

		colName = row.TableName()
	}

	return colName, nil
}
