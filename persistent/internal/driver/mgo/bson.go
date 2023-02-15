package mgo

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// Implements the `model.BSON` interface
type mgoBson bson.ObjectId

func (m *mgoBson) Hex() string {
	return bson.ObjectId(*m).Hex()
}

func (m *mgoBson) String() string {
	return bson.ObjectId(*m).String()
}

func (m *mgoBson) Valid() bool {
	return bson.ObjectId(*m).Valid()
}

func (m *mgoBson) MarshalJSON() ([]byte, error) {
	return bson.ObjectId(*m).MarshalJSON()
}

func (m *mgoBson) UnmarshalJSON(bytes []byte) error {
	var b bson.ObjectId
	err := b.UnmarshalJSON(bytes)
	*m = mgoBson(string(b))

	return err
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
