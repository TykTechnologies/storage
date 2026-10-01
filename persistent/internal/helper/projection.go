package helper

import (
	"reflect"
	"strings"
)

// ProjectionFields trims the names, keeps the first occurrence of each and skips a child path
// whose parent is also listed, so drivers build their projection from a clean list.
func ProjectionFields(fields []string) []string {
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

// NormalizeObjectIDs rewrites the _id of map results through normalize, which returns the
// replacement and true when the value is the driver's own id type. Any pointer to a map with string
// keys, or to a slice of such maps or of pointers to them, is handled, so model.DBM, plain maps and
// bson.M all qualify; typed destinations are left alone.
func NormalizeObjectIDs(result interface{}, normalize func(id interface{}) (interface{}, bool)) {
	value := reflect.ValueOf(result)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return
	}

	value = value.Elem()

	switch value.Kind() {
	case reflect.Map:
		normalizeObjectID(value, normalize)
	case reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			elem := value.Index(i)
			if elem.Kind() == reflect.Pointer && !elem.IsNil() {
				elem = elem.Elem()
			}

			normalizeObjectID(elem, normalize)
		}
	}
}

var idKey = reflect.ValueOf("_id")

func normalizeObjectID(row reflect.Value, normalize func(id interface{}) (interface{}, bool)) {
	if row.Kind() != reflect.Map || row.IsNil() || row.Type().Key().Kind() != reflect.String {
		return
	}

	id := row.MapIndex(idKey.Convert(row.Type().Key()))
	if !id.IsValid() || !id.CanInterface() {
		return
	}

	if replacement, ok := normalize(id.Interface()); ok {
		row.SetMapIndex(idKey.Convert(row.Type().Key()), reflect.ValueOf(replacement))
	}
}
