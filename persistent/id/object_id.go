package id

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"time"

	"gopkg.in/mgo.v2/bson"
)

type ObjectId string

func NewObjectID() ObjectId {
	return ObjectId(bson.NewObjectId())
}

// Valid returns true if id is valid. A valid id must contain exactly 12 bytes.
func (id ObjectId) Valid() bool {
	return len(id) == 12
}

func (id ObjectId) Hex() string {
	return hex.EncodeToString([]byte(id))
}

func (id ObjectId) String() string {
	return id.Hex()
	//	return fmt.Sprintf("ObjectID(%q)", id.Hex())
}

func (id ObjectId) Timestamp() time.Time {
	bytes := []byte(string(id)[0:4])
	secs := int64(binary.BigEndian.Uint32(bytes))

	return time.Unix(secs, 0)
}

func (id ObjectId) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.Hex())
}

func (id *ObjectId) UnmarshalJSON(buf []byte) error {
	var b bson.ObjectId
	err := b.UnmarshalJSON(buf)

	*id = ObjectId(string(b))

	return err
}

// ObjectIdHex useful to create an object ID from the string
func ObjectIdHex(id string) ObjectId {
	return ObjectId(bson.ObjectIdHex(id))
}

func IsObjectIdHex(s string) bool {
	if len(s) != 24 {
		return false
	}

	_, err := hex.DecodeString(s)

	return err == nil
}

// GetBSON only used by mgo
func (id ObjectId) GetBSON() (interface{}, error) {
	return bson.ObjectId(id), nil
}
