//go:build mongo
// +build mongo

package mgo

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestObjectIdHex(t *testing.T) {
	mgo := mgoDriver{}

	oldIdFormat := bson.NewObjectId()
	newIdFormat := mgo.ObjectIdHex(oldIdFormat.Hex())

	assert.Equal(t, oldIdFormat.String(), newIdFormat.String())
}
