package mgo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewObjectID(t *testing.T) {
	id := NewObjectID()

	assert.NotEqual(t, "", id)
}
