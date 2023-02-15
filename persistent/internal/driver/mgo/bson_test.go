//go:build mongo
// +build mongo

package mgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestNewObjectID(t *testing.T) {
	mgo := mgoDriver{}
	id := mgo.NewObjectID()

	objectToCompare := bson.ObjectIdHex(id.Hex())

	assert.Equal(t, objectToCompare.String(), id.String())
	assert.Equal(t, objectToCompare.Hex(), id.Hex())
	assert.Equal(t, objectToCompare.Time(), id.Timestamp())
	assert.Equal(t, objectToCompare.Valid(), id.Valid())
}
