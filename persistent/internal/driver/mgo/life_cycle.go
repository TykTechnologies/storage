package mgo

import (
	"crypto/tls"
	"errors"
	"github.com/TykTechnologies/storage/persistent/databaseinfo"
	"net"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

var _ model.StorageLifecycle = &lifeCycle{}

type lifeCycle struct {
	session *mgo.Session
	db      *mgo.Database
}

// Connect connects to the mongo database given the ClientOpts.
func (lc *lifeCycle) Connect(opts *model.ClientOpts) error {
	dialInfo, err := mgo.ParseURL(opts.ConnectionString)
	if err != nil {
		return err
	}

	dialInfo.Timeout = model.DEFAULT_CONN_TIMEOUT
	if opts.ConnectionTimeout != 0 {
		dialInfo.Timeout = time.Second * time.Duration(opts.ConnectionTimeout)
	}

	if opts.UseSSL {
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

	sess.SetSocketTimeout(dialInfo.Timeout)
	sess.SetSyncTimeout(dialInfo.Timeout)

	lc.session = sess

	lc.setSessionConsistency(opts)

	lc.db = lc.session.DB("")

	return nil
}

// Close finish the session.
func (lc *lifeCycle) Close() error {
	if lc.session != nil {
		lc.session.Close()

		lc.session = nil
		lc.db = nil

		return nil
	}

	return errors.New("closing a no connected database")
}

// DBType returns the type of the registered storage driver.
func (lc *lifeCycle) DBType() databaseinfo.DBType {
	var result struct {
		Code int `bson:"code"`
	}

	if err := lc.session.Run("features", &result); err != nil && result.Code == 303 {
		return databaseinfo.AWSDocumentDB
	}

	return databaseinfo.StandardMongo
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
