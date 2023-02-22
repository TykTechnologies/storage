package id

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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
	return fmt.Sprintf("ObjectID(%q)", id.Hex())
}

func (id OID) GetBSON() (interface{}, error) {
	return bson.ObjectId(id), nil
}

func (id OID) Timestamp() time.Time {
	bytes := []byte(string(id)[0:4])
	secs := int64(binary.BigEndian.Uint32(bytes))
	return time.Unix(secs, 0).UTC()
}

func (id OID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.Hex())
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
