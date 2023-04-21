package model

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestValidObjectID(t *testing.T) {
	valid, _ := hex.DecodeString("63efa41f4713944d6f696768")
	tcs := []struct {
		testName      string
		givenObjectID ObjectID
		expectedValid bool
	}{
		{
			testName:      "valid",
			givenObjectID: ObjectID(valid),
			expectedValid: true,
		},
		{
			testName:      "invalid",
			givenObjectID: ObjectID("test"),
			expectedValid: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			actual := tc.givenObjectID.Valid()

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
	assert.Equal(t, bsonID.Hex(), id.String())
}

func TestTimestamp(t *testing.T) {
	id := NewObjectID()
	bsonID := bson.ObjectId(id)

	assert.Equal(t, bsonID.Time(), id.Timestamp())
}

func TestTime(t *testing.T) {
	id := NewObjectID()
	bsonID := bson.ObjectId(id)

	assert.Equal(t, bsonID.Time(), id.Time())
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

	var id2 ObjectID
	err = id2.UnmarshalJSON(idBytes)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, id, id2)
}

func TestIsObjectIDHex(t *testing.T) {
	id := NewObjectID()

	assert.Equal(t, true, IsObjectIDHex(id.Hex()))
	assert.Equal(t, false, IsObjectIDHex("any-invalid-value"))
}

func TestValue(t *testing.T) {
	id := NewObjectID()
	val, err := id.Value()

	assert.Equal(t, nil, err)
	assert.Equal(t, id.Hex(), val)
}

func TestScan(t *testing.T) {
	cases := []struct {
		name      string
		arg       interface{}
		want      ObjectID
		shouldErr bool
	}{
		{"valid byte slice", []byte("641b80edd4aefc2c1e104bd1"), ObjectIDHex("641b80edd4aefc2c1e104bd1"), false},
		{"valid string", "641b80edd4aefc2c1e104bd1", ObjectIDHex("641b80edd4aefc2c1e104bd1"), false},
		{"invalid type", 123, ObjectID(""), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var objID ObjectID
			err := objID.Scan(tc.arg)

			if err != nil && !tc.shouldErr {
				t.Errorf("Scan(%v) returned an error: %v", tc.arg, err)
			}

			if objID.Hex() != tc.want.Hex() {
				t.Errorf("Scan(%v) = %v, want %v", tc.arg, objID, tc.want)
			}
		})
	}
}

func TestNewObjectIDWithTime(t *testing.T) {
	// Create a new time with a known Unix timestamp
	testTime := time.Date(2022, 3, 24, 12, 0, 0, 0, time.UTC)
	expectedHex := bson.NewObjectIdWithTime(testTime).Hex()

	// Call the function with the test time
	result := NewObjectIDWithTime(testTime)

	// Check that the result matches the expected hex string
	if result.Hex() != expectedHex {
		t.Errorf("NewObjectIDWithTime(%v) = %v, expected %v", testTime, result.Hex(), expectedHex)
	}
}
