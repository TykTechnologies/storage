package mongo

import "gopkg.in/mgo.v2"

type lifeCycle struct {
	session *mgo.Session
	db      *mgo.Database
}
