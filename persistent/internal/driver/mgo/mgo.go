package mgo

import (
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
func NewMgoDriver(opts *model.ClientOpts) (*mgoDriver, error){
	newDriver := &mgoDriver{}

	// create the db life cycle manager
	lc := &lifeCycle{}
	// connect to the db
	err := lc.Connect(opts)
	if err != nil{
		return nil, err
	}

	newDriver.lifeCycle = lc

	return newDriver, nil
}

func (d *mgoDriver) NewBSONID() model.BSON {
	id := bson.NewObjectId()
	return &mgoBson{id}
}

