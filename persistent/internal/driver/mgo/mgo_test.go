//go:build mongo
// +build mongo

package mgo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
)



type dummyDBObject struct {
	Id    id.OID `bson:"_id,omitempty"`
	Name  string   `bson:"name"`
	Email string   `bson:"email"`
}

func (d dummyDBObject) GetObjectID() id.OID {
	return d.Id
}

func (d *dummyDBObject) SetObjectID(id id.OID) {
	d.Id = id
}

func (d dummyDBObject) TableName() string {
	return "dummy"
}

func TestInsert(t *testing.T) {
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	assert.Nil(t, err)
	// create a new dummy table
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}

	// insert the object into the database
	err = mgo.Insert(context.Background(), object)
	assert.Nil(t, err)
	// delete the object from the database
	defer mgo.Delete(context.Background(),  object)

	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID() )
}

func TestDelete(t *testing.T) {
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	assert.Nil(t, err)
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}

	// insert the object into the database
	err = mgo.Insert(context.Background(), object)
	assert.Nil(t, err)
	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())

	// delete the object from the database
	err = mgo.Delete(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was deleted
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.NotNil(t, err)
	assert.True(t, mgo.IsErrNoRows(err))
}

func TestIsErrNoRows(t *testing.T) {
	mgoDriver := mgoDriver{}

	assert.True(t, mgoDriver.IsErrNoRows(mgo.ErrNotFound))
	assert.False(t, mgoDriver.IsErrNoRows(nil))
	assert.False(t, mgoDriver.IsErrNoRows(mgo.ErrCursor))
}
