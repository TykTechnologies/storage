package mgo

import (
	"time"

	"github.com/TykTechnologies/storage/persistent/internal/model"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type mgoDriver struct {
	session *mgo.Session
	db      *mgo.Database

	lastConnAttempt time.Time
	options         model.ClientOpts
}

// Connect connects to mongo given the ClientOpts
func (d *mgoDriver) Connect(opts *model.ClientOpts) error {
	if d.db != nil {
		if err := d.db.Session.Ping(); err == nil {
			return nil
		}
	}

	dialInfo, err := mgo.ParseURL(d.options.ConnectionString)
	if err != nil {
		return err
	}
	sess, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return err
	}

	d.session = sess

	d.setSessionConsistency()

	d.db = d.session.DB("")

	return nil
}

func (d *mgoDriver) NewBSONID() model.BSON {
	id := bson.NewObjectId()
	return &mgoBson{id}
}

func (d *mgoDriver) setSessionConsistency() {
	switch d.options.SessionConsistency {
	case "eventual":
		d.session.SetMode(mgo.Eventual, true)
	case "monotonic":
		d.session.SetMode(mgo.Monotonic, true)
	default:
		d.session.SetMode(mgo.Strong, true)
	}
}
