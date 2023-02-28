package mongo

import (
	"reflect"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/stretchr/testify/assert"
)

func TestCreateCustomRegistry(t *testing.T){
	customRegistry := createCustomRegistry()

	build := customRegistry.Build()

	encoder, err := build.LookupEncoder(reflect.TypeOf(id.NewObjectID()))
	assert.Nil(t, err)
	assert.NotNil(t, encoder)

	decoder, err := build.LookupDecoder(reflect.TypeOf(id.NewObjectID()))
	assert.Nil(t, err)
	assert.NotNil(t, decoder)
}
