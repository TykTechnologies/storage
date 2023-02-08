package mgo

import "testing"

func Test_NewBSONID(t *testing.T) {
	mgo := mgoDriver{}

	id := mgo.NewBSONID()

	t.Log(id)
	t.Log(id.Hex())
	t.Log(id.Timestamp())
}
