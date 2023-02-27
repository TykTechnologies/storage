package persistent

import (
	"errors"
	"github.com/TykTechnologies/storage/persistent/internal/driver/mongo"

	"github.com/TykTechnologies/storage/persistent/internal/driver/mgo"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

const (
	OfficialMongo string = "mongo-go"
	Mgo           string = "mgo"
)

type (
	ClientOpts        model.ClientOpts
	PersistentStorage model.PersistentStorage
)

// NewPersistentStorage returns a persistent storage object that uses the given driver
func NewPersistentStorage(opts *ClientOpts) (model.PersistentStorage, error) {
	clientOpts := model.ClientOpts(*opts)
	switch opts.Type {
	case OfficialMongo:
		return mongo.NewMongoDriver(&clientOpts)
	case Mgo:
		return mgo.NewMgoDriver(&clientOpts)
	default:
		return nil, errors.New("invalid driver")
	}
}
