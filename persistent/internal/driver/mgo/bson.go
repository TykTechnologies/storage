package mgo

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// inherit all the bson methods from mgo.v2. Implements the
// `model.BSON` interface
type mgoBson bson.ObjectId

func (m *mgoBson) Hex() string {
	return bson.ObjectId(*m).Hex()
}

func (m *mgoBson) String() string {
	return m.Hex()
}

func (m *mgoBson) GetBSON() (interface{}, error) {
	return bson.ObjectId(*m), nil
}

func (m *mgoBson) Valid() bool {
	return bson.ObjectId(*m).Valid()
}

func (m *mgoBson) MarshalJSON() ([]byte, error) {
	return bson.ObjectId(*m).MarshalJSON()
}

func (m *mgoBson) UnmarshalJSON(bytes []byte) error {
	var b bson.ObjectId
	b.UnmarshalJSON(bytes)
	*m = mgoBson(string(b))

	return nil
}

func (m *mgoBson) MarshalText() ([]byte, error) {
	bsonId := bson.ObjectId(*m)
	return bsonId.MarshalText()
}

func (m *mgoBson) UnmarshalText(bytes []byte) error {
	bsonId := bson.ObjectId(*m)
	return bsonId.UnmarshalText(bytes)
}

func (m *mgoBson) Timestamp() time.Time {
	return bson.ObjectId(*m).Time()
}
