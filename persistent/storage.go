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
func NewPersistentStorage(opts model.ClientOpts) (model.PersistentStorage, error) {
	switch opts.Type {
	case OfficialMongo:
		return nil, errors.New("not implemented")
	case Mgo:
		return nil, errors.New("not implemented")
	default:
		return nil, errors.New("invalid driver")
	}
}
