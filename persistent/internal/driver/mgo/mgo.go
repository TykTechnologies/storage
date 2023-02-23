package mgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
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
		row.SetObjectID(id.OID(bson.NewObjectId().Hex()))
	}

	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	return col.Insert(row)
}

func (d *mgoDriver) Delete(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	return col.Remove(row)
}

func (d *mgoDriver) Update(ctx context.Context, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	return col.UpdateId(row.GetObjectID(), row)
}

func (d *mgoDriver) Count(ctx context.Context, row id.DBObject) (int, error) {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(row.TableName())

	return col.Find(nil).Count()
}

func (d *mgoDriver) Query(ctx context.Context, row id.DBObject, result interface{}, query model.DBM) error {
	session := d.session.Copy()

	col := session.DB("").C(row.TableName())
	defer col.Database.Session.Close()

	search := d.getQuery(query, col)

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
		conErr := d.HandleStoreError(err)
		if conErr != nil {
			return fmt.Errorf("failed while reconnecting to mongo: %w", conErr)
		}

		return err
	}

	return nil
}

func (d *mgoDriver) getQuery(query model.DBM, col *mgo.Collection) bson.M {
	search := bson.M{}

	for k, v := range query {
		// Skip _hints and _sort fields
		if k == "_sort" {
			continue
		}

		if k == "_limit" {
			continue
		}

		if k == "_offset" {
			continue
		}

		// If user provided nested query, for example, "name": {"$ne": "123"}
		if nested, ok := v.(model.DBM); ok {
			found := false

			for nk, nv := range nested {
				// Mongo support it as it is
				if nk == "$in" {
					continue
				}

				if nk == "$i" {
					quoted := regexp.QuoteMeta(nv.(string))
					search[k] = &bson.RegEx{Pattern: fmt.Sprintf("^%s$", quoted), Options: "i"}
					found = true
				} else if nk == "$text" {
					search[k] = bson.M{"$regex": bson.RegEx{Pattern: regexp.QuoteMeta(nv.(string)), Options: "i"}}
					found = true
				}
			}

			if found {
				continue
			}
		}

		if k == "_id" {
			if id, ok := v.(bson.ObjectId); ok {
				search[k] = bson.ObjectId(id)
				continue
			}
		}

		if reflect.ValueOf(v).Kind() == reflect.Slice && k != "$or" {
			search[k] = bson.M{"$in": v}
		} else {
			switch v := v.(type) {
			case model.DBM:
				search[k] = d.getQuery(v, col)
			case []model.DBM:
				sliceOfBsons := []bson.M{}
				for _, sliceValue := range v {
					sliceOfBsons = append(sliceOfBsons, d.getQuery(sliceValue, col))
				}
				search[k] = sliceOfBsons
			default:
				search[k] = v
			}
		}
	}

	return search
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
				return fmt.Errorf("failure while connecting to mongo: %w", connErr)
			}

			return nil
		}
	}

	return nil
}

func (d *mgoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mgo.ErrNotFound)
}
