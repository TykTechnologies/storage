//go:build mongo
// +build mongo

package mgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewObjectID(t *testing.T) {
	mgo := mgoDriver{}
	id := mgo.NewObjectID()

	assert.NotEqual(t, "", id)
}
