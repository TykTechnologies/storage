package helper

import "reflect"

func IsSlice(o interface{}) bool {
	return reflect.TypeOf(o).Elem().Kind() == reflect.Slice
}
