package mongo

import (
	"reflect"
	"time"

	"github.com/TykTechnologies/storage/persistent/id"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonoptions"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/mgocompat"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// tOID is the type of id.ObjectID
var tOID = reflect.TypeOf(id.NewObjectID())

// toTime is the type of golang time.Time
var toTime = reflect.TypeOf(time.Time{})

// objectIDDecodeValue encode Hex value of id.ObjectId into primitive.ObjectID
func objectIDEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tOID {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: val}
	}

	s := val.Interface().(id.ObjectId).Hex()

	newOID, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return err
	}

	return vw.WriteObjectID(newOID)
}

// objectIDDecodeValue decode Hex value of primitive.ObjectID into id.ObjectId
func objectIDDecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	objectID, err := vr.ReadObjectID()
	if err != nil {
		return err
	}

	newOID := id.ObjectIdHex(objectID.Hex())

	if val.CanSet() {
		val.Set(reflect.ValueOf(newOID))
	}

	return nil
}

// createCustomRegistry creates a *bsoncodec.RegistryBuilder for our lifeCycle mongo's client using  objectIDDecodeValue
// and objectIDEncodeValue as Type Encoder/Decoders for id.ObjectId and time.Time
func createCustomRegistry() *bsoncodec.RegistryBuilder {
	// using mgocompat registry as base type registry
	rb := mgocompat.NewRegistryBuilder()

	// set the id.ObjectID encoders/decoders
	rb.RegisterTypeEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterTypeDecoder(tOID, bsoncodec.ValueDecoderFunc(objectIDDecodeValue))

	// we set the default behavior to use local time zone - the same as mgo does internally.
	UseLocalTimeZone := true
	opts := &bsonoptions.TimeCodecOptions{UseLocalTimeZone: &UseLocalTimeZone}
	// set the time.Time encoders/decoders
	rb.RegisterTypeDecoder(toTime, bsoncodec.NewTimeCodec(opts))
	rb.RegisterTypeEncoder(toTime, bsoncodec.NewTimeCodec(opts))

	return rb
}
