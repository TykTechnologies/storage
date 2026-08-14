//go:build mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package persistent_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent"
	"github.com/TykTechnologies/storage/persistent/internal/testutil"
	"github.com/TykTechnologies/storage/persistent/model"
)

type mgoConformanceEntity struct {
	ID   model.ObjectID `bson:"_id,omitempty"`
	Name string         `bson:"name"`
}

func (e *mgoConformanceEntity) TableName() string             { return "conformance_entities" }
func (e *mgoConformanceEntity) GetObjectID() model.ObjectID   { return e.ID }
func (e *mgoConformanceEntity) SetObjectID(id model.ObjectID) { e.ID = id }

func TestConformanceMgo(t *testing.T) {
	storage, err := persistent.NewPersistentStorage(&persistent.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		Type:             persistent.Mgo,
	})
	require.NoError(t, err)
	require.NoError(t, storage.Ping(context.Background()))

	testutil.RunSuite(t, testutil.Suite{
		Storage:   storage,
		NewObject: func() model.DBObject { return &mgoConformanceEntity{} },
		IDFilter:  func(id model.ObjectID) model.DBM { return model.DBM{"_id": id} },
	})
}
