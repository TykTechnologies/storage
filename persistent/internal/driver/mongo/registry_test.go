package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/stretchr/testify/assert"
)

func TestCreateCustomRegistry(t *testing.T) {
	customRegistry := createCustomRegistry()

	build := customRegistry.Build()

	encoder, err := build.LookupEncoder(reflect.TypeOf(id.NewObjectID()))
	assert.Nil(t, err)
	assert.NotNil(t, encoder)

	decoder, err := build.LookupDecoder(reflect.TypeOf(id.NewObjectID()))
	assert.Nil(t, err)
	assert.NotNil(t, decoder)
}

type TestStruct struct {
	Id id.ObjectId
	MapVal map[string]interface{}
	InterfaceSliceVal []interface{}
	StringSliceVal []string
}

func (d TestStruct) GetObjectID() id.ObjectId {
	return d.Id
}

func (d *TestStruct) SetObjectID(id id.ObjectId) {
	d.Id = id
}

func (d TestStruct) TableName() string {
	return "dummy"
}
func TestStructValues(t *testing.T){
	driver, _ := prepareEnvironment(t)
	defer cleanDB(t)

	testObj := TestStruct{
		Id: id.NewObjectID(),
	}
	ctx := context.Background()
	err := driver.Insert(ctx, &testObj)
	assert.Equal(t, nil, err)

	newObj:= TestStruct{}
	err = driver.Query(ctx, &newObj, &newObj, dbm.DBM{})
	assert.Nil(t, err)

	byt, _ := json.Marshal(&newObj)
	fmt.Println(string(byt))

}