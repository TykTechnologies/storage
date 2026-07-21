//go:build mongo7 || mongo6 || mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo7 mongo6 mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mongo

import (
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/testutil"
	"github.com/TykTechnologies/storage/persistent/model"
)

func TestConformance(t *testing.T) {
	driver, _ := prepareEnvironment(t)
	defer cleanDB(t)

	testutil.RunSuite(t, testutil.Suite{
		Storage:   driver,
		NewObject: func() model.DBObject { return &dummyDBObject{} },
		IDFilter:  func(id model.ObjectID) model.DBM { return model.DBM{"_id": id} },
	})
}
