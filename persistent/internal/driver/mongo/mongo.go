package mongo

import (
	"errors"
	"time"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

type mongoDriver struct {
	*lifeCycle
	lastConnAttempt time.Time
	options         model.ClientOpts
}

// NewMongoDriver returns an instance of the driver official mongo connected to the database.
func NewMongoDriver(opts *model.ClientOpts) (*mongoDriver, error) {
	if opts.ConnectionString == "" {
		return nil, errors.New("can't connect without connection string")
	}

	newDriver := &mongoDriver{}

	// create the db life cycle manager
	lc := &lifeCycle{}

	newDriver.lifeCycle = lc

	return newDriver, nil
}
