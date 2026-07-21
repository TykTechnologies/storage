//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/testutil"
	"github.com/TykTechnologies/storage/persistent/model"
)

func TestConformance(t *testing.T) {
	driver, _ := setupTest(t)
	defer teardownTest(t, driver)

	testutil.RunSuite(t, testutil.Suite{
		Storage:   driver,
		NewObject: func() model.DBObject { return &TestObject{} },
		IDFilter:  func(id model.ObjectID) model.DBM { return model.DBM{"id": id} },
	})
}
