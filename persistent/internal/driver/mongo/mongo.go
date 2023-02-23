package mongo

import (
	"context"
	"errors"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoDriver struct {
	*lifeCycle
	options *model.ClientOpts
}

// NewMongoDriver returns an instance of the driver official mongo connected to the database.
func NewMongoDriver(opts *model.ClientOpts) (*mongoDriver, error) {
	if opts.ConnectionString == "" {
		return nil, errors.New("can't connect without connection string")
	}

	newDriver := &mongoDriver{}
	newDriver.options = opts

	// create the db life cycle manager
	lc := &lifeCycle{}

	if err := lc.Connect(opts); err != nil {
		return nil, err
	}

	newDriver.lifeCycle = lc

	return newDriver, nil
}

func (d *mongoDriver) Insert(ctx context.Context, row id.DBObject) error {
	if row.GetObjectID() == "" {
		row.SetObjectID(id.OID(primitive.NewObjectID().String()))
	}

	collection := d.client.Database(d.database).Collection(row.TableName())

	_, err := collection.InsertOne(ctx, row)

	return err
}

func (d *mongoDriver) Delete(ctx context.Context, row id.DBObject) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	res, err := collection.DeleteOne(ctx, bson.M{"_id": row.GetObjectID()})
	if err != nil {
		return err
	}

	if res.DeletedCount == 0 {
		return errors.New("error deleting a non existing object")
	}

	return nil
}

func (d *mongoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}
