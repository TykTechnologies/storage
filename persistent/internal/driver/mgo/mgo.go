package mgo

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/persistent/id"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/model"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

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

func (d *mgoDriver) Insert(ctx context.Context, row id.DBObject) error {
	if row.GetObjectID() == "" {
		row.SetObjectID(id.NewObjectID())
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	err := col.Insert(row)
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return nil
}

func (d *mgoDriver) Delete(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	err := col.Remove(row)
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return nil
}

func (d *mgoDriver) Update(ctx context.Context, row id.DBObject, query ...model.DBM) error {
	sess := d.session.Copy()
	defer sess.Close()

	if len(query) > 1 {
		return errors.New("multiple queries for only 1 row")
	}

	if len(query) == 0 {
		query = append(query, model.DBM{"$set": row})
	}

	colName, err := getColName(query[0], row)
	if err != nil {
		return err
	}

	col := sess.DB("").C(colName)

	finalQuery := []bson.M{}
	for i := range query {
		finalQuery = append(finalQuery, buildQuery(query[i]))
	}

	return col.Update(bson.M{"_id": row.GetObjectID()}, finalQuery)
}

func (d *mgoDriver) UpdateMany(ctx context.Context, rows []id.DBObject, query ...model.DBM) error {
	// if no query is provided, we assume that the rows are already updated
	if len(query) == 0 {
		if len(rows) == 0 {
			return errors.New("no rows provided")
		}

		sess := d.session.Copy()
		defer sess.Close()

		colName := rows[0].TableName()
		col := sess.DB("").C(colName)
		bulk := col.Bulk()

		for _, row := range rows {
			if row.TableName() != colName {
				return errors.New("rows must be of the same collection")
			}

			query := model.DBM{"$set": row}
			bulk.Update(bson.M{"_id": row.GetObjectID()}, buildQuery(query))
		}

		res, err := bulk.Run()
		if err != nil {
			return err
		}

		if res.Modified == 0 {
			return mgo.ErrNotFound
		}

		return nil
	}

	if len(rows) != len(query) {
		return errors.New("different length of row and query")
	}

	sess := d.session.Copy()
	defer sess.Close()

	colName, err := getColName(query[0], rows[0])
	if err != nil {
		return err
	}

	col := sess.DB("").C(colName)
	bulk := col.Bulk()

	for i := range rows {
		newColName, err := getColName(query[i], rows[i])
		if err != nil {
			return err
		}

		if newColName != colName {
			return errors.New("rows must be of the same collection")
		}

		bulk.Update(bson.M{"_id": rows[i].GetObjectID()}, buildQuery(query[i]))
	}

	res, err := bulk.Run()
	if err != nil {
		return err
	}

	if res.Modified == 0 {
		return mgo.ErrNotFound
	}

	return nil
}

func (d *mgoDriver) Count(ctx context.Context, row id.DBObject) (int, error) {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	n, err := col.Find(nil).Count()
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return 0, rErr
		}

		return 0, err
	}

	return n, nil
}

func (d *mgoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query model.DBM) error {
	session := d.session.Copy()

	colName, err := getColName(query, row)
	if err != nil {
		return err
	}

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

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

	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}

		return err
	}

	return err
}

func (d *mgoDriver) DeleteWhere(ctx context.Context, row id.DBObject, query model.DBM) error {
	session := d.session.Copy()

	colName, err := getColName(query, row)
	if err != nil {
		return err
	}

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

	_, err = col.RemoveAll(buildQuery(query))
	if err != nil {
		rErr := d.handleStoreError(err)
		if rErr != nil {
			return rErr
		}
	}

	return err
}

func (d *mgoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mgo.ErrNotFound)
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

			return nil
		}
	}

	return nil
}
