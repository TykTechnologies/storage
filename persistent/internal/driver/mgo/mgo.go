package mgo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/persistent/id"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/model"

	"gopkg.in/mgo.v2"
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
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return errors.New("error reconnecting to mongo: " + rErr.Error() + " after Insert error: " + err.Error())
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
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return errors.New("error reconnecting to mongo: " + rErr.Error() + " after Delete error: " + err.Error())
		}

		return err
	}

	return nil
}

func (d *mgoDriver) Update(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	err := col.UpdateId(row.GetObjectID(), row)
	if err != nil {
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return errors.New("error reconnecting to mongo: " + rErr.Error() + " after Update error: " + err.Error())
		}

		return err
	}

	return nil
}

func (d *mgoDriver) Count(ctx context.Context, row id.DBObject) (int, error) {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	n, err := col.Find(nil).Count()
	if err != nil {
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return 0, fmt.Errorf("error reconnecting to mongo: %w after Count error: %v", rErr, err)
		}

		return 0, err
	}

	return n, nil
}

func (d *mgoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query model.DBM) error {
	session := d.session.Copy()

	colName, ok := query["_collection"].(string)
	if !ok {
		colName = row.TableName()
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

	var err error
	if helper.IsSlice(result) {
		err = q.All(result)
	} else {
		err = q.One(result)
	}

	if err != nil {
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return errors.New("error reconnecting to mongo: " + rErr.Error() + " after Query error: " + err.Error())
		}

		return err
	}

	return err
}

func (d *mgoDriver) DeleteWhere(ctx context.Context, row id.DBObject, query model.DBM) error {
	session := d.session.Copy()

	colName, ok := query["_collection"].(string)
	if !ok {
		colName = row.TableName()
	}

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

	_, err := col.RemoveAll(buildQuery(query))
	if err != nil {
		rErr := d.HandleStoreError(err)
		if rErr != nil {
			return errors.New("error reconnecting to mongo: " + rErr.Error() + " after DeleteWhere error: " + err.Error())
		}
	}

	return err
}

func (d *mgoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mgo.ErrNotFound)
}

func (d *mgoDriver) HandleStoreError(err error) error {
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
				return connErr
			}

			return nil
		}
	}

	return nil
}
