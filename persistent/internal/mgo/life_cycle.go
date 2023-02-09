package mgo

import (
	"crypto/tls"
	"net"

	"github.com/TykTechnologies/storage/persistent/internal/model"
	"gopkg.in/mgo.v2"
)

var _ model.StorageLifecycle = &lifeCycle{}

type lifeCycle struct {
	session *mgo.Session
	db      *mgo.Database
}

func (lc *lifeCycle) Connect(opts *model.ClientOpts) error {
	dialInfo, err := mgo.ParseURL(opts.ConnectionString)
	if err != nil {
		return err
	}

	dialInfo.Timeout = model.DEFAULT_CONN_TIMEOUT

	if opts.UseSSL{
		tlsConfig, err := opts.GetTLSConfig()
		if err != nil {
			return err
		}

		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		}

	}

	sess, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return err
	}

	lc.session = sess

	lc.setSessionConsistency(opts)

	lc.db = lc.session.DB("")

	return nil
}

func (lc *lifeCycle) Close() error{
	return nil
}

// DBType returns the type of the registered storage driver.
func (lc *lifeCycle) DBType() model.DBType{
	return model.MongoType
}

func (lc *lifeCycle) setSessionConsistency(opts *model.ClientOpts) {
	switch opts.SessionConsistency {
	case "eventual":
		lc.session.SetMode(mgo.Eventual, true)
	case "monotonic":
		lc.session.SetMode(mgo.Monotonic, true)
	default:
		lc.session.SetMode(mgo.Strong, true)
	}
}