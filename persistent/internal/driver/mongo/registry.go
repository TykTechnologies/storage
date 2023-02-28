package mongo

import (
	"reflect"

	"github.com/TykTechnologies/storage/persistent/id"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var tOID = reflect.TypeOf(id.NewObjectID())

func objectIDEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tOID {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: val}
	}
	s := val.Interface().(id.ObjectId).Hex()
	newOID, _ := primitive.ObjectIDFromHex(s)
	return vw.WriteObjectID(newOID)
}

func objectIDDecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	objectID, err := vr.ReadObjectID()
	if err != nil{
		return err
	}
	newOID := id.ObjectIdHex(objectID.Hex())

	val.Set(reflect.ValueOf(newOID))
	return nil
}

func createCustomRegistry() *bsoncodec.RegistryBuilder {
	var primitiveCodecs bson.PrimitiveCodecs
	rb := bsoncodec.NewRegistryBuilder()
	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)
	rb.RegisterTypeEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterTypeDecoder(tOID, bsoncodec.ValueDecoderFunc(objectIDDecodeValue))
	primitiveCodecs.RegisterPrimitiveCodecs(rb)
	return rb
}