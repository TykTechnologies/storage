package mgo

import (
	"gopkg.in/mgo.v2/bson"
	"time"
)

// inherit all the bson methods from mgo.v2. Implements the
// `mode.BSON` interface
type mgoBson struct {
	bson.ObjectId
}

func (m *mgoBson) Timestamp() time.Time {
	return m.Time()
}
