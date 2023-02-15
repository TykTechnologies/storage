package model

import (
	"time"
)

type DBType string

const (
	StandardMongo DBType = "mongo"
	AWSDocumentDB DBType = "docdb"
)

type StorageLifecycle interface {
	// Connects to a db instance
	Connect(*ClientOpts) error

	// Close cleans up possible resources after we stop using storage driver.
	Close() error

	// DBType returns the type of the registered storage driver.
	DBType() DBType
}

// DBTable is an interface that should be implemented by
// database models in order to perform CRUD operations
type DBTable interface {
	TableName() string
}

// ObjectID interface to be implemented by each db driver
type ObjectID interface {
	Hex() string
	String() string
	Timestamp() time.Time
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	MarshalText() ([]byte, error)
	UnmarshalText([]byte) error
}

type DBObject interface {
	DBID() ObjectID
	SetDBID(id ObjectID)
}
