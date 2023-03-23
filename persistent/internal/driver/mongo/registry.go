package mongo

import (
	"reflect"

	"github.com/TykTechnologies/storage/persistent/id"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonoptions"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
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
	var primitiveCodecs bson.PrimitiveCodecs
	rb := bsoncodec.NewRegistryBuilder()
	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)
	structcodec, _ := bsoncodec.NewStructCodec(bsoncodec.DefaultStructTagParser,
		bsonoptions.StructCodec().
			SetDecodeZeroStruct(true).
			SetEncodeOmitDefaultStruct(true).
			SetOverwriteDuplicatedInlinedFields(false).
			SetAllowUnexportedFields(true))

	mapCodec := bsoncodec.NewMapCodec(
		bsonoptions.MapCodec().
			SetDecodeZerosMap(true).
			SetEncodeNilAsEmpty(true).
			SetEncodeKeysWithStringer(true))



	rb.RegisterDefaultEncoder(reflect.Slice, bsoncodec.NewSliceCodec(bsonoptions.SliceCodec().SetEncodeNilAsEmpty(true)))
	rb.RegisterTypeEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterTypeDecoder(tOID, bsoncodec.ValueDecoderFunc(objectIDDecodeValue))
	rb.RegisterDefaultEncoder(reflect.Map, mapCodec)
	rb.RegisterDefaultDecoder(reflect.Map, mapCodec)
	rb.RegisterDefaultDecoder(reflect.Struct, structcodec)
	rb.RegisterDefaultEncoder(reflect.Struct, structcodec)
	primitiveCodecs.RegisterPrimitiveCodecs(rb)


	return rb
}
