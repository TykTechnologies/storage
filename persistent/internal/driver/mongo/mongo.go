package mongo

import (
	"context"
	"errors"
	"fmt"

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

func (d *mongoDriver) Update(ctx context.Context, object id.DBObject) error {
	panic("implement me")
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
		fmt.Println("ERROR CONNECTINGGG::::", err)
		return nil, err
	}

	newDriver.lifeCycle = lc

	return newDriver, nil
}

func (d *mongoDriver) Insert(ctx context.Context, row id.DBObject) error {
	if row.GetObjectID() == "" {
		row.SetObjectID(id.ObjectId(primitive.NewObjectID().String()))
	}

	db := d.database
	fmt.Println("with database:",db)

	table := row.TableName()
	fmt.Println("with table:",table)

	fmt.Println("with client:",d.client)
	collection := d.client.Database(db).Collection(table)

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

func (d *mongoDriver) Count(ctx context.Context, row id.DBObject) (int, error) {
	collection := d.client.Database(d.database).Collection(row.TableName())

	count, err := collection.CountDocuments(ctx, bson.D{})

	return int(count), err
}

func (d *mongoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}
