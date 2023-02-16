package mgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestObjectIdHex(t *testing.T) {
	mgo := mgoDriver{}

	oldIdFormat := bson.NewObjectId()
	newIdFormat := mgo.ObjectIdHex(oldIdFormat.Hex())

	assert.Equal(t, oldIdFormat.String(), newIdFormat.String())
}

func TestIsErrNoRows(t *testing.T) {
	mgoDriver := mgoDriver{}

	assert.True(t, mgoDriver.IsErrNoRows(mgo.ErrNotFound))
	assert.False(t, mgoDriver.IsErrNoRows(nil))
	assert.False(t, mgoDriver.IsErrNoRows(mgo.ErrCursor))

}
