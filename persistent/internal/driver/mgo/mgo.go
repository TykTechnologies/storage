package mgo

import (
	"context"
	"time"

	"github.com/TykTechnologies/storage/persistent/id"

	"github.com/TykTechnologies/storage/persistent/internal/model"

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

func (d *mgoDriver) NewObjectID() id.ObjectID {
	id := bson.NewObjectId()
	mgoId := mgoBson(id)
	return &mgoId
}

func (d *mgoDriver) ObjectIdHex(s string) id.ObjectID {
	hex := mgoBson(bson.ObjectIdHex(s))
	return &hex
}

func (d *mgoDriver) Insert(ctx context.Context, table model.DBTable, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(table.TableName())

	return col.Insert(row)
}

func (d *mgoDriver) Delete(ctx context.Context, table model.DBTable, row id.DBObject) error {
	sess := d.session.Copy()
	defer sess.Close()

	col := sess.DB("").C(table.TableName())

	return col.Remove(row)
}
