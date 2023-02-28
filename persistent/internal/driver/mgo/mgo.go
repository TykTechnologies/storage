package mgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
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

	search := d.buildQuery(query)

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

	return err
}

func (m *mgoDriver) buildQuery(query model.DBM) bson.M {
	search := bson.M{}

	for key, value := range query {
		switch key {
		case "_sort", "_collection", "_limit", "_offset", "_date_sharding":
			continue
		case "_id":
			if id, ok := value.(bson.ObjectId); ok {
				search[key] = bson.ObjectId(id)
				continue
			}
			handleQueryValue(key, value, search)
		default:
			handleQueryValue(key, value, search)
		}
	}
	return search
}

func handleQueryValue(key string, value interface{}, search bson.M) {
	if isNestedQuery(value) {
		handleNestedQuery(search, key, value)
	} else if reflect.ValueOf(value).Kind() == reflect.Slice && key != "$or" {
		strSlice, isStr := value.([]string)

		if isStr && key == "_id" {
			objectIDs := []bson.ObjectId{}
			for _, str := range strSlice {
				objectIDs = append(objectIDs, bson.ObjectIdHex(str))
			}
			search[key] = bson.M{"$in": objectIDs}
			return
		}
		search[key] = bson.M{"$in": value}
	} else {
		search[key] = value
	}
}

func isNestedQuery(value interface{}) bool {
	_, ok := value.(model.DBM)
	return ok
}

func handleNestedQuery(search bson.M, key string, value interface{}) {
	nestedQuery, ok := value.(model.DBM)
	if !ok {
		return
	}

	for nestedKey, nestedValue := range nestedQuery {
		switch nestedKey {
		case "$i":
			if stringValue, ok := nestedValue.(string); ok {
				quoted := regexp.QuoteMeta(stringValue)
				search[key] = &bson.RegEx{Pattern: fmt.Sprintf("^%s$", quoted), Options: "i"}
			}
		case "$text":
			if stringValue, ok := nestedValue.(string); ok {
				search[key] = bson.M{"$regex": bson.RegEx{Pattern: regexp.QuoteMeta(stringValue), Options: "i"}}
			}
		default:
			search[key] = bson.M{nestedKey: nestedValue}
		}
	}
}

func (d *mgoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mgo.ErrNotFound)
}
