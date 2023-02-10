package persistent

import (
	"errors"
	"github.com/TykTechnologies/storage/persistent/internal/driver/mgo"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

const (
	OfficialMongo string = "mongo-go"
	Mgo           string = "mgo"
)

type ClientOpts model.ClientOpts
type PersistentStorage model.PersistentStorage
type ObjectID model.ObjectID

// NewPersistentStorage returns a persistent storage object that uses the given driver
func NewPersistentStorage(opts *ClientOpts) (model.PersistentStorage, error) {
	clientOpts := model.ClientOpts(*opts)
	switch opts.Type {
	case OfficialMongo:
		return nil, errors.New("not implemented")
	case Mgo:
		driver, err := mgo.NewMgoDriver(&clientOpts)
		return driver, err
	default:
		return nil, errors.New("invalid driver")
	}
}
