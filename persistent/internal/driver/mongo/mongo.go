package mongo

import (
	"context"
	"errors"

	"github.com/TykTechnologies/storage/persistent/dbm"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/index"
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

	return d.handleStoreError(err)
}

func (d *mongoDriver) Delete(ctx context.Context, row id.DBObject, query ...dbm.DBM) error {
	if len(query) > 1 {
		return errors.New(model.ErrorMultipleQueryForSingleRow)
	}

	if len(query) == 0 {
		query = append(query, dbm.DBM{"_id": row.GetObjectID()})
	}

	collection := d.client.Database(d.database).Collection(row.TableName())

	result, err := collection.DeleteMany(ctx, buildQuery(query[0]))

	if err == nil && result.DeletedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return d.handleStoreError(err)
}

func (d *mongoDriver) Count(ctx context.Context, row id.DBObject) (int, error) {
	collection := d.client.Database(d.database).Collection(row.TableName())

	count, err := collection.CountDocuments(ctx, bson.D{})

	return int(count), d.handleStoreError(err)
}

func (d *mongoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}

func (d *mongoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query dbm.DBM) error {
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

	return d.handleStoreError(err)
}

func (d *mongoDriver) Drop(ctx context.Context, row id.DBObject) error {
	collection := d.client.Database(d.database).Collection(row.TableName())

	return d.handleStoreError(collection.Drop(ctx))
}

func (d *mongoDriver) Update(ctx context.Context, row id.DBObject, query ...dbm.DBM) error {
	if len(query) > 1 {
		return errors.New(model.ErrorMultipleQueryForSingleRow)
	}

	if len(query) == 0 {
		query = append(query, dbm.DBM{"_id": row.GetObjectID()})
	}

	collection := d.client.Database(d.database).Collection(row.TableName())

	result, err := collection.UpdateOne(ctx, buildQuery(query[0]), bson.D{{Key: "$set", Value: row}})
	if err == nil && result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return d.handleStoreError(err)
}

func (d *mongoDriver) UpdateMany(ctx context.Context, rows []id.DBObject, query ...dbm.DBM) error {
	if len(query) > 0 && len(query) != len(rows) {
		return errors.New(model.ErrorRowQueryDiffLenght)
	}

	if len(rows) == 0 {
		return errors.New(model.ErrorEmptyRow)
	}

	var bulkQuery []mongo.WriteModel

	for i := range rows {
		update := mongo.NewUpdateOneModel().SetUpdate(bson.D{{Key: "$set", Value: rows[i]}})

		if len(query) == 0 {
			update.SetFilter(dbm.DBM{"_id": rows[i].GetObjectID()})
		} else {
			update.SetFilter(query[i])
		}

		bulkQuery = append(bulkQuery, update)
	}

	collection := d.client.Database(d.database).Collection(rows[0].TableName())

	result, err := collection.BulkWrite(ctx, bulkQuery)
	if err == nil && result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return d.handleStoreError(err)
}

func (d *mongoDriver) HasTable(ctx context.Context, collection string) (bool, error) {
	collections, err := d.client.Database(d.database).ListCollectionNames(ctx, bson.M{"name": collection})
	return len(collections) > 0, err
}

func (d *mongoDriver) Ping(ctx context.Context) error {
	return d.handleStoreError(d.client.Ping(ctx, nil))
}

func (d *mongoDriver) handleStoreError(err error) error {
	if err == nil {
		return nil
	}

	// Check for a mongo.ServerError or any of its underlying wrapped errors
	var serverErr mongo.ServerError
	// Check if the error is a network error
	if mongo.IsNetworkError(err) || errors.As(err, &serverErr) {
		// Reconnect to the MongoDB instance
		if connErr := d.Connect(d.options); connErr != nil {
			return errors.New(model.ErrorReconnecting + ": " + connErr.Error() + " after error: " + err.Error())
		}
	}

	return err
}

func (d *mongoDriver) CreateIndex(ctx context.Context, row id.DBObject, index index.Index) error {
	if len(index.Keys) == 0 {
		return errors.New(model.ErrorIndexEmpty)
	} else if len(index.Keys) > 1 && index.IsTTLIndex {
		return errors.New(model.ErrorIndexComposedTTL)
	}

	keys := bson.D{}

	for _, key := range index.Keys {
		builtQuery := buildQuery(key)
		for name, val := range builtQuery {
			keys = append(keys, bson.E{Key: name, Value: val})
		}
	}

	opts := options.Index()

	//nolint:staticcheck
	opts.SetBackground(index.Background)

	if name := index.Name; name != "" {
		opts.SetName(name)
	}

	if index.IsTTLIndex {
		opts.SetExpireAfterSeconds(int32(index.TTL))
	}

	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: opts,
	}

	collection := d.client.Database(d.database).Collection(row.TableName())

	_, err := collection.Indexes().CreateOne(ctx, indexModel)

	return err
}

func (d *mongoDriver) GetIndexes(ctx context.Context, row id.DBObject) ([]index.Index, error) {
	collection := d.client.Database(d.database).Collection(row.TableName())

	var indexes []index.Index

	indexesSpec, err := collection.Indexes().ListSpecifications(ctx)
	if err != nil {
		return indexes, err
	}

	// parse from mongo IndexSpec to our index.Index again
	for _, thisIndex := range indexesSpec {
		bsonKeys := bson.D{}

		if errUnmarshal := bson.Unmarshal(thisIndex.KeysDocument, &bsonKeys); err != nil {
			return indexes, errUnmarshal
		}

		var newKeys []dbm.DBM

		for _, v := range bsonKeys {
			newKey := dbm.DBM{}
			newKey[v.Key] = v.Value

			newKeys = append(newKeys, newKey)
		}

		newIndex := index.Index{
			Name: thisIndex.Name,
			Keys: newKeys,
		}

		if TTL := thisIndex.ExpireAfterSeconds; TTL != nil {
			newIndex.TTL = int(*TTL)
			newIndex.IsTTLIndex = true
		}

		indexes = append(indexes, newIndex)
	}

	return indexes, nil
}

func (d *mongoDriver) AutoMigrate(ctx context.Context, rows []id.DBObject, opts ...dbm.DBM) error {
	if len(opts) > 0 && len(opts) != len(rows) {
		return errors.New(model.ErrorRowOptDiffLenght)
	}

	for i, row := range rows {
		has, err := d.HasTable(ctx, row.TableName())
		if err != nil {
			return errors.New("error looking for table: " + err.Error())
		}

		if !has {
			if len(opts) > 0 {
				opt := buildOpt(opts[i])

				err := d.client.Database(d.database).CreateCollection(ctx, row.TableName(), opt)
				if err != nil {
					return err
				}

				continue
			}

			err := d.client.Database(d.database).CreateCollection(ctx, row.TableName())
			if err != nil {
				return err
			}
		}
	}

	return nil
}
