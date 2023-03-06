package mongo

import (
	"context"
	"errors"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
		row.SetObjectID(id.NewObjectID())
	}

	collection := d.client.Database(d.database).Collection(row.TableName())

	_, err := collection.InsertOne(ctx, row)
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return nil
}

func (d *mongoDriver) Delete(ctx context.Context, row id.DBObject) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	res, err := collection.DeleteOne(ctx, bson.M{"_id": row.GetObjectID()})
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

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
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return 0, rErr
		}

		return 0, err
	}

	return int(count), nil
}

func (d *mongoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}

func (d *mongoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query model.DBM) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	search := buildQuery(query)

	findOpts := options.Find()
	findOneOpts := options.FindOne()

	sort, sortFound := query["_sort"].(string)
	if sortFound && sort != "" {
		sortQuery := buildLimitQuery(sort)
		findOpts.SetSort(sortQuery)
		findOneOpts.SetSort(sortQuery)
	}

	if limit, ok := query["_limit"].(int); ok && limit > 0 {
		findOpts.SetLimit(int64(limit))
	}

	if offset, ok := query["_offset"].(int); ok && offset > 0 {
		findOpts.SetSkip(int64(offset))
		findOneOpts.SetSkip(int64(offset))
	}

	var err error

	if helper.IsSlice(result) {
		var cursor *mongo.Cursor

		cursor, err = collection.Find(ctx, search, findOpts)
		if err == nil {
			err = cursor.All(ctx, result)
		}
		defer cursor.Close(ctx)
	} else {
		err = collection.FindOne(ctx, search, findOneOpts).Decode(result)
	}

	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return nil
}

func (d *mongoDriver) Drop(ctx context.Context, row id.DBObject) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	err := collection.Drop(ctx)
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return nil
}

// 0 ... N
// Update(ctx,row)
// Update(ctx, row, dbm)
// Update(ctx, row, []dbm)
func (d *mongoDriver) Update(ctx context.Context, row id.DBObject, query ...model.DBM) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	if len(query) > 1 {
		return errors.New("multiple queries for only 1 row")
	}

	if len(query) == 0 {
		query = append(query, model.DBM{"_id": row.GetObjectID()})
	}

	result, err := collection.UpdateOne(ctx, query[0], bson.D{{Key: "$set", Value: row}})
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (d *mongoDriver) UpdateMany(ctx context.Context, rows []id.DBObject, query ...model.DBM) error {
	if len(query) > 0 && len(query) != len(rows) {
		return errors.New("query and row lens should be the same")
	}

	var bulkQuery []mongo.WriteModel

	for i := range rows {
		update := mongo.NewUpdateOneModel().SetUpdate(bson.D{{Key: "$set", Value: rows[i]}})

		if len(query) == 0 {
			update.SetFilter(model.DBM{"_id": rows[i].GetObjectID()})
		} else {
			update.SetFilter(query[i])
		}

		bulkQuery = append(bulkQuery, update)
	}

	collection := d.client.Database(d.database).Collection(rows[0].TableName())

	result, err := collection.BulkWrite(ctx, bulkQuery)
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (d *mongoDriver) DeleteWhere(ctx context.Context, row id.DBObject, query model.DBM) error {
	colName, ok := query["_collection"].(string)
	if !ok {
		colName = row.TableName()
	}

	collection := d.client.Database(d.database).Collection(colName)

	result, err := collection.DeleteMany(ctx, buildQuery(query))
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	if result.DeletedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (d *mongoDriver) handleStoreError(err error) error {
	if err == nil {
		return nil
	}

	// Check if the error is a network error
	if mongo.IsNetworkError(err) {
		// Reconnect to the MongoDB instance
		if connErr := d.Connect(d.options); connErr != nil {
			return errors.New("error reconnecting to mongo: " + connErr.Error() + " after error: " + err.Error())
		}

		return nil
	}

	// Check for a mongo.ServerError or any of its underlying wrapped errors
	var serverErr mongo.ServerError
	if errors.As(err, &serverErr) {
		// Reconnect to the MongoDB instance
		if connErr := d.Connect(d.options); connErr != nil {
			return errors.New("error reconnecting to mongo: " + connErr.Error() + " after error: " + err.Error())
		}

		return nil
	}

	return nil
}
