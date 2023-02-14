package mgo

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/id"
	"time"

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
	return &mgoBson{id}
}

func (d *mgoDriver) Insert(ctx context.Context, table model.DBTable, row id.DBObject) error {
	sess := d.session.Copy()

	col := sess.DB("").C(table.TableName())
	defer col.Database.Session.Close()

	return col.Insert(row)
}
