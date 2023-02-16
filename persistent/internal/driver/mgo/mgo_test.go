//go:build mongo
// +build mongo

package mgo

import (

	"context"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

type dummyTable struct{}

func (d dummyTable) TableName() string {
	return "dummy"
}

type dummyDBObject struct {
	Id    *mgoBson `bson:"_id,omitempty"`
	Name  string   `bson:"name"`
	Email string   `bson:"email"`
}

func (d dummyDBObject) GetObjectID() id.ObjectID {
	return d.Id
}

func (d *dummyDBObject) SetObjectID(id id.ObjectID) {
	d.Id = id.(*mgoBson)
}

func TestInsert(t *testing.T) {
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	assert.Nil(t, err)
	// create a new dummy table
	var table dummyTable
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}
	objId := mgo.NewObjectID()
	object.SetObjectID(objId)

	// insert the object into the database
	err = mgo.Insert(context.Background(), table, object)
	assert.Nil(t, err)
	// delete the object from the database
	defer mgo.Delete(context.Background(), table, object)

	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(table.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID().String(), result.GetObjectID().String())

}

func TestDelete(t *testing.T) {
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	assert.Nil(t, err)
	// create a new dummy table
	var table dummyTable
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}

	object.SetObjectID(mgo.NewObjectID())

	// insert the object into the database
	err = mgo.Insert(context.Background(), table, object)
	assert.Nil(t, err)
	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(table.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID().String(), result.GetObjectID().String())

	// delete the object from the database
	err = mgo.Delete(context.Background(), table, object)
	assert.Nil(t, err)

	// check if the object was deleted
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.NotNil(t, err)
}

func TestObjectIdHex(t *testing.T) {
	mgo := mgoDriver{}

	oldIdFormat := bson.NewObjectId()
	newIdFormat := mgo.ObjectIdHex(oldIdFormat.Hex())

	assert.Equal(t, oldIdFormat.String(), newIdFormat.String())
}
