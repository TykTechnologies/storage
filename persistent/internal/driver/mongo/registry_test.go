//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
)

func TestCreateCustomRegistry(t *testing.T) {
	customRegistry := createCustomRegistry()

	build := customRegistry.Build()

	encoder, err := build.LookupEncoder(reflect.TypeOf(model.NewObjectId()))
	assert.Nil(t, err)
	assert.NotNil(t, encoder)

	decoder, err := build.LookupDecoder(reflect.TypeOf(model.NewObjectId()))
	assert.Nil(t, err)
	assert.NotNil(t, decoder)
}

type testStruct struct {
	Id                model.ObjectId
	MapVal            map[string]interface{}
	InterfaceSliceVal []interface{}
	StringSliceVal    []string
	DBMMap            []model.DBM
	Timestamp         time.Time
}

func (d *testStruct) GetObjectId() model.ObjectId {
	return d.Id
}

func (d *testStruct) SetObjectId(id model.ObjectId) {
	d.Id = id
}

func (d *testStruct) TableName() string {
	return "dummy"
}

func TestStructValues(t *testing.T) {
	driver, _ := prepareEnvironment(t)
	defer cleanDB(t)

	currentTime := time.Date(2023, 0o4, 0o4, 10, 0, 0, 0, time.Local)
	testObj := testStruct{
		Id:        model.NewObjectId(),
		DBMMap:    []model.DBM{{"test": "a"}},
		Timestamp: currentTime,
	}

	ctx := context.Background()
	err := driver.Insert(ctx, &testObj)
	assert.Equal(t, nil, err)

	newObj := testStruct{}
	err = driver.Query(ctx, &newObj, &newObj, model.DBM{})
	assert.Nil(t, err)

	byt, err := json.Marshal(&newObj)
	assert.Nil(t, err)

	result := string(byt)

	assert.Contains(t, result, "\"MapVal\":{}")
	assert.Contains(t, result, "\"StringSliceVal\":[]")
	assert.Contains(t, result, "\"InterfaceSliceVal\":[]")
	assert.Contains(t, result, "\"DBMMap\":[{\"test\":\"a\"}]")
	assert.Contains(t, result, "\"Timestamp\":\"", currentTime.String(), "\"")
}
