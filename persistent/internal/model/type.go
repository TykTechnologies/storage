package model

type DBType string

type StorageLifecycle interface {
	// Connects to a db instance
	Connect(ClientOpts) error

	// Close cleans up possible resources after we stop using storage driver.
	Close() error

	// DBType returns the type of the registered storage driver.
	DBType() DBType
}
