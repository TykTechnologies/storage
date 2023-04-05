//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

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

type testStruct struct {
	Id                id.ObjectId
	MapVal            map[string]interface{}
	InterfaceSliceVal []interface{}
	StringSliceVal    []string
	DBMMap            []dbm.DBM
	Timestamp         time.Time
}

func (d *testStruct) GetObjectID() id.ObjectId {
	return d.Id
}

func (d *testStruct) SetObjectID(id id.ObjectId) {
	d.Id = id
}

func (d *testStruct) TableName() string {
	return "dummy"
}

func TestStructValues(t *testing.T) {
	driver, _ := prepareEnvironment(t)
	defer cleanDB(t)

	currentTime := time.Date(2023, 0o4, 0o4, 10, 0, 0, 0, time.UTC)
	testObj := testStruct{
		Id:        id.NewObjectID(),
		DBMMap:    []dbm.DBM{{"test": "a"}},
		Timestamp: currentTime,
	}

	ctx := context.Background()
	err := driver.Insert(ctx, &testObj)
	assert.Equal(t, nil, err)

	newObj := testStruct{}
	err = driver.Query(ctx, &newObj, &newObj, dbm.DBM{})
	assert.Nil(t, err)

	byt, err := json.Marshal(&newObj)
	assert.Nil(t, err)

	result := string(byt)

	assert.Contains(t, result, "\"MapVal\":{}")
	assert.Contains(t, result, "\"StringSliceVal\":[]")
	assert.Contains(t, result, "\"InterfaceSliceVal\":[]")
	assert.Contains(t, result, "\"DBMMap\":[{\"test\":\"a\"}]")
	assert.Contains(t, result, "\"Timestamp\":\"2023-04-04T12:00:00+02:00\"")
}
