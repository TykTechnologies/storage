package mongo

import (
	"reflect"

	"github.com/TykTechnologies/storage/persistent/id"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/mgocompat"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var tOID = reflect.TypeOf(id.NewObjectID())

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
// and objectIDEncodeValue as Type Encoder/Decoders for id.ObjectId
func createCustomRegistry() *bsoncodec.RegistryBuilder {
	rb := mgocompat.NewRegistryBuilder()


	rb.RegisterTypeEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterTypeDecoder(tOID, bsoncodec.ValueDecoderFunc(objectIDDecodeValue))


	return rb
}
