package helper

import (
	"log"
	"reflect"
	"strings"
)

func IsSlice(o interface{}) bool {
	return reflect.TypeOf(o).Elem().Kind() == reflect.Slice
}

func ErrPrint(err error) {
	if err != nil {
		log.Println(err.Error())
	}
}

func IsCosmosDB(connectionString string) bool {
	return strings.Contains(connectionString, ".cosmos.")
}
