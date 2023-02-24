package id

import (
	"encoding/hex"
	"fmt"
	"testing"

	"gopkg.in/mgo.v2/bson"

	"github.com/stretchr/testify/assert"
)

func TestValidObjectId(t *testing.T) {
	valid, _ := hex.DecodeString("63efa41f4713944d6f696768")
	tcs := []struct {
		testName      string
		givenObjectId ObjectId
		expectedValid bool
	}{
		{
			testName:      "valid",
			givenObjectId: ObjectId(valid),
			expectedValid: true,
		},
		{
			testName:      "invalid",
			givenObjectId: ObjectId("test"),
			expectedValid: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			actual := tc.givenObjectId.Valid()

			assert.Equal(t, tc.expectedValid, actual)
		})
	}
}

func TestNewObjectID(t *testing.T) {
	id := NewObjectID()
	assert.Equal(t, 12, len(id))
}

func TestHex(t *testing.T) {
	id := NewObjectID()
	expected := hex.EncodeToString([]byte(id))
	assert.Equal(t, expected, id.Hex())
}

func TestString(t *testing.T) {
	id := NewObjectID()
	bsonID := bson.ObjectId(id)
	assert.Equal(t, fmt.Sprintf("ObjectID(%q)", bsonID.Hex()), id.String())
}

func TestTimestamp(t *testing.T) {
	id := NewObjectID()
	bsonID := bson.ObjectId(id)

	assert.Equal(t, bsonID.Time(), id.Timestamp())
}

func TestMarshalJSON(t *testing.T) {
	id := NewObjectID()
	bsonID := bson.ObjectId(id)

	bsonBytes, err1 := bsonID.MarshalJSON()
	idBytes, err2 := id.MarshalJSON()

	if err1 != nil || err2 != nil {
		t.Fatal("failed marshaling object id")
	}

	assert.Equal(t, bsonBytes, idBytes)
}

func TestUnmarshalJSON(t *testing.T) {
	id := NewObjectID()
	idBytes, err := id.MarshalJSON()

	if err != nil {
		t.Fatal(err)
	}

	var id2 ObjectId
	err = id2.UnmarshalJSON(idBytes)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, id, id2)
}

func TestIsObjectIdHex(t *testing.T) {
	id := NewObjectID()

	assert.Equal(t, true, IsObjectIdHex(id.Hex()))
	assert.Equal(t, false, IsObjectIdHex("any-invalid-value"))
}
