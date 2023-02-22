package id

import (
	"encoding/hex"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type OID string

// Valid returns true if id is valid. A valid id must contain exactly 12 bytes.
func (id OID) Valid() bool {
	return len(id) == 12
}

func (id OID) Hex() string {
	return hex.EncodeToString([]byte(id))
}

func (id OID) String() string {
	return id.Hex()
}

func (id OID) GetBSON() (interface{}, error) {
	return string(id), nil
}

func (id OID) Timestamp() time.Time {
	return bson.ObjectId(id).Time()
}

func (id OID) MarshalJSON() ([]byte, error) {

	x, err := bson.ObjectId(id).MarshalJSON()
	fmt.Printf("\n Lo marshalleado: %+v\n", string(x))
	return x, err
}

func (id *OID) UnmarshalJSON(buf []byte) error {
	var b bson.ObjectId
	b.UnmarshalJSON(buf)
	*id = OID(string(b))

	return nil
}

// ObjectIdHex useful to create an object ID from the string
func ObjectIdHex(id string) OID {
	return OID(bson.ObjectIdHex(id))
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
