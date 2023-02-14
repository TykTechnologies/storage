package id

import "time"

// ObjectID interface to be implemented by each db driver
type ObjectID interface {
	Hex() string
	String() string
	Timestamp() time.Time
	Valid() bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	MarshalText() ([]byte, error)
	UnmarshalText([]byte) error
}

type DBObject interface {
	GetObjectID() ObjectID
	SetObjectID(id ObjectID)
}
