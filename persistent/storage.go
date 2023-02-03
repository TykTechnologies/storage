package persistent

import (
	"errors"
	"github.com/TykTechnologies/storage/persistent/internal/model"
)

const (
	OfficialMongo string = "mongo-go"
	Mgo           string = "mgo"
)

// NewPersistentStorage returns a persistent storage object that uses the given driver
func NewPersistentStorage(opts model.ClientOpts) error {
	switch opts.Type {
	case OfficialMongo:
		// to be implemented
	case Mgo:
		// To be implemented
	default:
		return errors.New("invalid driver")
	}
}