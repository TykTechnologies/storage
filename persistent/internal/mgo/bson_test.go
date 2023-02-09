package mgo

import "testing"

func Test_NewBSONID(t *testing.T) {
	id := NewObjectID()

	t.Log(id)
	t.Log(id.Hex())
	t.Log(id.Timestamp())
}
