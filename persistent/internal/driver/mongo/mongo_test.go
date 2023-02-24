//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"errors"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

type dummyDBObject struct {
	Id    id.OID `bson:"_id,omitempty"`
	Name  string `bson:"name"`
	Email string `bson:"email"`
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

func prepareEnvironment(t *testing.T) (*mongoDriver, *dummyDBObject) {
	t.Helper()
	// create a new mgo driver connection
	mgo, err := NewMongoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	if err != nil {
		t.Fatal(err)
	}
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}

	return mgo, object
}

func TestNewMongoDriver(t *testing.T) {
	t.Run("new driver with connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "mongodb://localhost:27017/test",
		})

		assert.Nil(t, err)
		assert.NotNil(t, newDriver)
		assert.NotNil(t, newDriver.lifeCycle)
		assert.NotNil(t, newDriver.options)
		assert.Nil(t, newDriver.client.Ping(context.Background(), nil))
	})
	t.Run("new driver with invalid connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "test",
		})

		assert.NotNil(t, err)
		assert.Equal(t, "invalid connection string", err.Error())
		assert.Nil(t, newDriver)
	})
	t.Run("new driver without connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{})

		assert.NotNil(t, err)
		assert.Equal(t, "can't connect without connection string", err.Error())
		assert.Nil(t, newDriver)
	})
}

func TestInsert(t *testing.T) {
	driver, object := prepareEnvironment(t)
	ctx := context.Background()
	collection := driver.client.Database("test").Collection(object.TableName())

	// insert the object into the database
	err := driver.Insert(ctx, object)
	assert.Nil(t, err)
	// delete the collection
	defer collection.Drop(ctx)

	// check if the object was inserted
	var result dummyDBObject
	err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestDelete(t *testing.T) {
	driver, object := prepareEnvironment(t)
	ctx := context.Background()
	collection := driver.client.Database("test").Collection(object.TableName())

	t.Run("deleting a existing object", func(t *testing.T) {
		// insert the object into the database
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		// delete the collection
		defer collection.Drop(ctx)

		// validates that the object was inserted
		var result dummyDBObject
		err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
		assert.Nil(t, err)
		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())

		// delete the object from the database
		err = driver.Delete(ctx, object)
		assert.Nil(t, err)

		// check if the object was deleted
		err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})

	t.Run("deleting a non existent object", func(t *testing.T) {
		// delete the object from the database
		err := driver.Delete(ctx, object)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New("error deleting a non existing object"), err)
	})
}
