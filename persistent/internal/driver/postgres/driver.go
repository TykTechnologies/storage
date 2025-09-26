package postgres

import (
	"errors"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	_ "github.com/lib/pq"
)

var _ types.PersistentStorage = &driver{}

type driver struct {
	*lifeCycle
	options       *types.ClientOpts
	TableSharding bool
}

// NewPostgresDriver initializes a new PostgreSQL driver with the given client options.
// Returns the driver instance or an error if the connection setup fails.
func NewPostgresDriver(opts *types.ClientOpts) (*driver, error) {
	lc := &lifeCycle{}

	driver := &driver{}
	driver.lifeCycle = lc
	err := driver.Connect(opts)
	if err != nil {
		return nil, err
	}
	return driver, nil
}

func (d *driver) validateDBAndTable(object model.DBObject) (string, error) {
	// Check if the database connection is valid
	if d.db == nil {
		return "", errors.New(types.ErrorSessionClosed)
	}

	if object == nil {
		return "", errors.New(types.ErrorNilObject)
	}
	// Get the table name from the DBObject
	tableName := object.TableName()
	if tableName == "" {
		return "", errors.New(types.ErrorEmptyTableName)
	}

	return tableName, nil
}
