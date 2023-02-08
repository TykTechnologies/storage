package model

import "time"

type DBType string
const(
	MongoType DBType = "mongo"
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

// BSON interface to be implemented by each mongo driver
type BSON interface {
	Hex() string
	String() string
	Timestamp() time.Time
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	MarshalText() ([]byte, error)
	UnmarshalText([]byte) error
}
