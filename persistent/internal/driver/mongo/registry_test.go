package mongo

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/persistent/id"
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
