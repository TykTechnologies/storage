package helper

import (
	"log"
	"reflect"
)

func IsSlice(o interface{}) bool {
	return reflect.TypeOf(o).Kind() == reflect.Slice
}

func ErrPrint(err error) {
	if err != nil {
		log.Println(err.Error())
	}
}
