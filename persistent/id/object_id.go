package id

import (
	"gopkg.in/mgo.v2/bson"
	"time"
)

type OID string

// Valid returns true if id is valid. A valid id must contain exactly 12 bytes.
func (id OID) Valid() bool {
	return len(id) == 12
}

func (id OID) Hex() string {
	if string(id) == "" {
		return ""
	}

	idObj := bson.ObjectIdHex(string(id))
	return idObj.Hex()
}

func (id OID) String() string {
	idObj := bson.ObjectIdHex(string(id))
	return idObj.String()
}

func (id OID) Timestamp() time.Time {
	idObj := bson.ObjectIdHex(string(id))
	return idObj.Time()
}

type ObjectID interface {
	Hex() string
	String() string
	Timestamp() time.Time
	Valid() bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	MarshalText() ([]byte, error)
	UnmarshalText([]byte) error
}
