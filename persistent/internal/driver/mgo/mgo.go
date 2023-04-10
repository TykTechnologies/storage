package mgo

import (
	"context"
	"errors"
	"fmt"
	"gopkg.in/mgo.v2"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/index"
	"github.com/TykTechnologies/storage/persistent/utils"

	"gopkg.in/mgo.v2/bson"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/model"
)

var _ model.PersistentStorage = &mgoDriver{}

type mgoDriver struct {
	*lifeCycle
	lastConnAttempt time.Time
	options         model.ClientOpts
}

// NewMgoDriver returns an instance of the driver connected to the database.
func NewMgoDriver(opts *model.ClientOpts) (*mgoDriver, error) {
	newDriver := &mgoDriver{}

	// create the db life cycle manager
	lc := &lifeCycle{}
	// connect to the db
	err := lc.Connect(opts)
	if err != nil {
		return nil, err
	}

	newDriver.lifeCycle = lc

	return newDriver, nil
}

func (d *mgoDriver) Insert(ctx context.Context, rows ...id.DBObject) error {
	if len(rows) == 0 {
		return errors.New(model.ErrorEmptyRow)
	}

	sess := d.session.Copy()
	defer sess.Close()

	colName := rows[0].TableName()
	col := sess.DB("").C(colName)
	bulk := col.Bulk()

	for _, row := range rows {
		if row.GetObjectID() == "" {
			row.SetObjectID(id.NewObjectID())
		}

		bulk.Insert(row)
	}

	_, err := bulk.Run()

	return d.handleStoreError(err)
}

func (d *mgoDriver) Delete(ctx context.Context, row id.DBObject, queries ...dbm.DBM) error {
	if len(queries) > 1 {
		return errors.New(model.ErrorMultipleQueryForSingleRow)
	}

	if len(queries) == 0 {
		queries = append(queries, dbm.DBM{"_id": row.GetObjectID()})
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	res, err := col.RemoveAll(buildQuery(queries[0]))

	if err == nil && res.Removed == 0 {
		return mgo.ErrNotFound
	}

	return d.handleStoreError(err)
}

func (d *mgoDriver) Update(ctx context.Context, row id.DBObject, queries ...dbm.DBM) error {
	if len(queries) > 1 {
		return errors.New(model.ErrorMultipleQueryForSingleRow)
	}

	if len(queries) == 0 {
		queries = append(queries, dbm.DBM{"_id": row.GetObjectID()})
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	return d.handleStoreError(col.Update(buildQuery(queries[0]), bson.M{"$set": row}))
}

func (d *mgoDriver) BulkUpdate(ctx context.Context, rows []id.DBObject, query ...dbm.DBM) error {
	if len(rows) == 0 {
		return errors.New(model.ErrorEmptyRow)
	}

	if len(rows) != len(query) && len(query) != 0 {
		return errors.New(model.ErrorRowQueryDiffLenght)
	}

	sess := d.session.Copy()
	defer sess.Close()

	colName := rows[0].TableName()
	col := sess.DB("").C(colName)
	bulk := col.Bulk()

	for i := range rows {
		if len(query) == 0 {
			bulk.Update(bson.M{"_id": rows[i].GetObjectID()}, bson.M{"$set": rows[i]})

			continue
		}

		bulk.Update(buildQuery(query[i]), bson.M{"$set": rows[i]})
	}

	res, err := bulk.Run()
	if err == nil && res.Modified == 0 {
		return mgo.ErrNotFound
	}

	return d.handleStoreError(err)
}

func (d *mgoDriver) UpdateAll(ctx context.Context, row id.DBObject, query, update dbm.DBM) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	result, err := col.UpdateAll(buildQuery(query), buildQuery(update))
	if err == nil && result.Matched == 0 {
		return mgo.ErrNotFound
	}

	return d.handleStoreError(err)
}

func (d *mgoDriver) Count(ctx context.Context, row id.DBObject, filters ...dbm.DBM) (int, error) {
	if len(filters) > 1 {
		return 0, errors.New(model.ErrorMultipleDBM)
	}

	filter := bson.M{}
	if len(filters) == 1 {
		filter = buildQuery(filters[0])
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	n, err := col.Find(filter).Count()

	return n, d.handleStoreError(err)
}

func (d *mgoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query dbm.DBM) error {
	session := d.session.Copy()
	defer session.Close()

	colName, err := getColName(query, row)
	if err != nil {
		return err
	}

	col := session.DB("").C(colName)

	search := buildQuery(query)

	q := col.Find(search)

	sort, sortFound := query["_sort"].(string)
	if sortFound {
		q = q.Sort(sort)
	}

	if limit, ok := query["_limit"].(int); ok && limit > 0 {
		q = q.Limit(limit)
	}

	if offset, ok := query["_offset"].(int); ok && offset > 0 {
		q = q.Skip(offset)
	}

	if helper.IsSlice(result) {
		err = q.All(result)
	} else {
		err = q.One(result)
	}

	return d.handleStoreError(err)
}

func (d *mgoDriver) Drop(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	return d.handleStoreError(sess.DB("").C(row.TableName()).DropCollection())
}

func (d *mgoDriver) Ping(ctx context.Context) (result error) {
	if d.session == nil {
		return errors.New(model.ErrorSessionClosed)
	}

	defer func() {
		if err := recover(); err != nil {
			result = errors.New(model.ErrorSessionClosed + " from panic")
		}
	}()

	sess := d.session.Copy()
	defer sess.Close()

	return d.handleStoreError(sess.Ping())
}

func (d *mgoDriver) HasTable(ctx context.Context, collection string) (result bool, errResult error) {
	if d.session == nil {
		return false, errors.New(model.ErrorSessionClosed)
	}

	defer func() {
		if err := recover(); err != nil {
			errResult = errors.New(model.ErrorSessionClosed + " from panic")
		}
	}()

	sess := d.session.Copy()
	defer sess.Close()

	names, err := sess.DB("").CollectionNames()
	if err != nil {
		return false, err
	}

	for _, name := range names {
		if name == collection {
			return true, nil
		}
	}

	return false, nil
}

func (d *mgoDriver) handleStoreError(err error) error {
	if err == nil {
		return nil
	}

	listOfErrors := []string{
		"EOF",
		"Closed explicitly",
		"reset by peer",
		"no reachable servers",
		"i/o timeout",
	}

	for _, substr := range listOfErrors {
		if strings.Contains(err.Error(), substr) {
			connErr := d.Connect(&d.options)
			if connErr != nil {
				return errors.New("error reconnecting to mongo: " + connErr.Error() + " after error: " + err.Error())
			}

			return err
		}
	}

	return err
}

func (d *mgoDriver) CreateIndex(ctx context.Context, row id.DBObject, index index.Index) error {
	if len(index.Keys) == 0 {
		return errors.New(model.ErrorIndexEmpty)
	} else if len(index.Keys) > 1 && index.IsTTLIndex {
		return errors.New(model.ErrorIndexComposedTTL)
	}

	var indexes []string

	for _, key := range index.Keys {
		for k, v := range key {
			switch v.(type) {
			case int, int32, int64:
				if v.(int) == -1 {
					indexes = append(indexes, "-"+k)
				} else {
					indexes = append(indexes, k)
				}
			default:
				indexes = append(indexes, k+"_"+fmt.Sprint(v))
			}
		}
	}

	newIndex := mgo.Index{
		Name: index.Name,
		Key:  indexes,
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	if index.IsTTLIndex {
		newIndex.ExpireAfter = time.Duration(index.TTL) * time.Second
	}

	return col.EnsureIndex(newIndex)
}

func (d *mgoDriver) GetIndexes(ctx context.Context, row id.DBObject) ([]index.Index, error) {
	hasTable, err := d.HasTable(ctx, row.TableName())
	if err != nil {
		return nil, err
	}

	if !hasTable {
		return nil, errors.New(model.ErrorCollectionNotFound)
	}

	var indexes []index.Index

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	indexesSpec, err := col.Indexes()
	if err != nil {
		return indexes, err
	}

	for i := range indexesSpec {
		var newKeys []dbm.DBM

		for _, strKey := range indexesSpec[i].Key {
			newKey := dbm.DBM{}

			switch {
			case strings.HasPrefix(strKey, "-"):
				newKey[strKey[1:]] = int32(-1)
			case strKey != "_id" && strings.Contains(strKey, "_"):
				values := strings.Split(strKey, "_")
				newKey[values[0]] = values[1]
			default:
				newKey[strKey] = int32(1)
			}

			newKeys = append(newKeys, newKey)
		}

		newIndex := index.Index{
			Name: indexesSpec[i].Name,
			Keys: newKeys,
		}

		if indexesSpec[i].ExpireAfter > 0 {
			newIndex.IsTTLIndex = true
			newIndex.TTL = int(indexesSpec[i].ExpireAfter.Seconds())
		}

		indexes = append(indexes, newIndex)
	}

	return indexes, nil
}

func (d *mgoDriver) Migrate(ctx context.Context, rows []id.DBObject, opts ...dbm.DBM) error {
	sess := d.session.Copy()
	defer sess.Close()

	if len(opts) > 0 && len(opts) != len(rows) {
		return errors.New(model.ErrorRowOptDiffLenght)
	}

	for i, row := range rows {
		col := sess.DB("").C(row.TableName())

		if len(opts) > 0 {
			opt := buildOpt(opts[i])

			err := col.Create(opt)

			if d.handleStoreError(err) != nil {
				return err
			}

			continue
		}

		err := col.Create(&mgo.CollectionInfo{})

		if d.handleStoreError(err) != nil {
			return err
		}
	}

	return nil
}

func (d *mgoDriver) DropDatabase(ctx context.Context) error {
	sess := d.session.Copy()
	defer sess.Close()

	return sess.DB("").DropDatabase()
}

func (d *mgoDriver) DBTableStats(ctx context.Context, row id.DBObject) (dbm.DBM, error) {
	var stats dbm.DBM

	sess := d.session.Copy()
	defer sess.Close()

	err := sess.DB("").Run(dbm.DBM{"collStats": row.TableName()}, &stats)

	return stats, err
}

func (d *mgoDriver) Aggregate(ctx context.Context, row id.DBObject, query []dbm.DBM) ([]dbm.DBM, error) {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())
	pipe := col.Pipe(query)
	pipe.AllowDiskUse()
	iter := pipe.Iter()

	resultSlice := make([]dbm.DBM, 0)

	for {
		var result dbm.DBM
		if !iter.Next(&result) {
			break
		}
		// Parsing _id from bson.ObjectId to id.ObjectId
		resultId, ok := result["_id"].(bson.ObjectId)
		if ok {
			result["_id"] = id.ObjectIdHex(resultId.Hex())
		}

		resultSlice = append(resultSlice, result)
	}

	if iter.Err() != nil {
		return nil, iter.Err()
	}

	return resultSlice, nil
}

func (d *mgoDriver) CleanIndexes(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	indexes, err := col.Indexes()
	if err != nil {
		return err
	}

	for i := 0; i < len(indexes); i++ {
		index := &indexes[i]      // using pointers to avoid copying and improve performance
		if index.Name != "_id_" { // cannot drop _id index
			err = col.DropIndexName(index.Name)
			if err != nil {
				return d.handleStoreError(err)
			}
		}
	}

	return nil
}

func (d *mgoDriver) Upsert(ctx context.Context, row id.DBObject, query, update dbm.DBM) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	_, err := col.Find(query).Apply(mgo.Change{
		Update:    update,
		Upsert:    true,
		ReturnNew: true,
	}, row)

	return d.handleStoreError(err)
}

func (d *mgoDriver) GetDatabaseInfo(ctx context.Context) (utils.Info, error) {
	result := utils.Info{}

	db := d.session.DB("admin")
	err := db.Run(bson.D{{Name: "buildInfo", Value: 1}}, &result)
	result.Type = d.lifeCycle.DBType()

	return result, err
}

func (d *mgoDriver) GetCollections(ctx context.Context) ([]string, error) {
	return d.db.CollectionNames()
}
