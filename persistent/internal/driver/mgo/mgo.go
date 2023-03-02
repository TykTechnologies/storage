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
		row.SetObjectID(id.NewObjectID())
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

	colName, ok := query["_collection"].(string)
	if !ok {
		colName = row.TableName()
	}

	col := session.DB("").C(colName)
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
			if id, ok := value.(id.ObjectId); ok {
				search[key] = id
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
	switch {
	case isNestedQuery(value):
		handleNestedQuery(search, key, value)
	case reflect.ValueOf(value).Kind() == reflect.Slice && key != "$or":
		strSlice, isStr := value.([]string)

		if isStr && key == "_id" {
			objectIDs := []id.ObjectId{}
			for _, str := range strSlice {
				objectIDs = append(objectIDs, id.ObjectIdHex(str))
			}

			search[key] = bson.M{"$in": objectIDs}

			return
		}

		search[key] = bson.M{"$in": value}
	default:
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

func (d *mgoDriver) DeleteWhere(ctx context.Context, row id.DBObject, query model.DBM) error {
	session := d.session.Copy()

	colName, ok := query["_collection"].(string)
	if !ok {
		colName = row.TableName()
	}

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

	_, err := col.RemoveAll(d.buildQuery(query))

	return err
}

func (d *mgoDriver) UpdateWhere(context.Context, id.DBObject, model.DBM, model.DBM) error {
	panic("implement me")
}

func (d *mgoDriver) UpdateWhere_DBM(ctx context.Context, query interface{}, object interface{}) error {
	session := d.session.Copy()
	colName := "dummy"
	var err error

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

	switch queryWithType := query.(type) {
	case model.DBM:
		// We don't need to bulk update if the query is a single object
		_, err = col.UpdateAll(d.buildQuery(queryWithType), object)
		return err
	case []model.DBM:
		// Check if the object is also a slice
		bulk := col.Bulk()
		if sliceObject, ok := object.([]model.DBM); ok {
			if len(sliceObject) != len(queryWithType) {
				return fmt.Errorf("query and object must have the same length")
			}

			for i, q := range queryWithType {
				bulk.UpdateAll(d.buildQuery(q), d.buildQuery(sliceObject[i]))
			}

			_, err = bulk.Run()
			return err
		}

		// If it's not an slice, update with the same object the result of all the queries
		if obj, ok := object.(model.DBM); ok {
			for _, q := range queryWithType {
				bulk.UpdateAll(d.buildQuery(q), d.buildQuery(obj))
			}

			_, err = bulk.Run()
			return err
		}
		return errors.New("object must be of type model.DBM or []model.DBM")

	case nil:
		// Just update all the values if not query is provided
		_, err = col.UpdateAll(nil, object)
		return err
	default:
		return errors.New("query must be a model.DBM, []model.DBM or nil")
	}
}

func (d *mgoDriver) IsErrNoRows(err error) bool {
	return errors.Is(err, mgo.ErrNotFound)
}

func (d *mgoDriver) UpdateWhere_DBObject(ctx context.Context, query interface{}, object interface{}) error {
	session := d.session.Copy()
	colName := "dummy"

	var err error

	col := session.DB("").C(colName)
	defer col.Database.Session.Close()

	switch queryWithType := query.(type) {
	case model.DBM:
		// We don't need to bulk update if the query is a single object
		object, ok := object.(id.DBObject)
		if !ok {
			return errors.New("object must be of type id.DBObject")
		}

		result := make([]*id.DBObject, 0)

		err = d.Query(ctx, object, result, queryWithType)
		if err != nil {
			return err
		}

		if len(result) == 0 {
			return errors.New("no rows found")
		}
		bulk := col.Bulk()
		for _, r := range result {
			bulk.Update(r, r)
		}
		_, err = bulk.Run()

		return err
	case []model.DBM:
		bulk := col.Bulk()
		// Check if the object is also a slice
		if sliceObject, ok := object.([]id.DBObject); ok {
			if len(sliceObject) != len(queryWithType) {
				return errors.New("query and object must have the same length and ")
			}

			for i, q := range queryWithType {
				bulk.UpdateAll(d.buildQuery(q), sliceObject[i])
			}

			_, err = bulk.Run()
			return err
		}
		// If the object is not a slice, return an error because mgo doesn't support multi update with replacement-style update
		return errors.New("object must be of type []id.DBObject if query is []model.DBM: multi update is not supported for replacement-style update")

	// case nil:
	// 	bulk := col.Bulk()
	// 	if reflect.TypeOf(object).Kind() == reflect.Slice {
	// 		sliceObject := reflect.ValueOf(object)
	// 		for i := 0; i < sliceObject.Len(); i++ {
	// 			bulk.Update(sliceObject.Index(i).Interface(), sliceObject.Index(i).Interface())
	// 		}

	// 		_, err = bulk.Run()
	// 		return err
	// 	}

	// 	return nil
	default:
		return errors.New("query must be a model.DBM, []model.DBM or nil")
	}
}

func getCollection(result interface{}) (string, error) {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr {
		return "", fmt.Errorf("result argument must be a pointer")
	}

	if resultv.Elem().Kind() == reflect.Slice {
		resultv = reflect.New(resultv.Elem().Type().Elem())
		if resultv.Elem().Kind() == reflect.Ptr {
			resultv = resultv.Elem()
		}
	}

	return resultv.MethodByName("TableName").Call([]reflect.Value{})[0].Interface().(string), nil
}
