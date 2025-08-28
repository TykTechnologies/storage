//go:build postgres || postgres14 || postgres13 || postgres12.22 || postgres11 || postgres10
// +build postgres postgres14 postgres13 postgres12.22 postgres11 postgres10

package postgres

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type TestObject struct {
	ID        model.ObjectID `json:"id" gorm:"primaryKey"`
	Name      string         `json:"name"`
	Value     int            `json:"value"`
	CreatedAt time.Time      `json:"created_at"`
}

func (t *TestObject) TableName() string {
	return "test_objects"
}

func (t *TestObject) GetObjectID() model.ObjectID {
	return t.ID
}

func (t *TestObject) SetObjectID(id model.ObjectID) {
	t.ID = id
}

func setupTest(t *testing.T) (*driver, context.Context) {
	ctx := context.Background()

	// Use the same hardcoded values as in the Taskfile
	connStr := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
	opts := &types.ClientOpts{
		ConnectionString: connStr,
		Type:             "postgres",
	}

	driver, err := NewPostgresDriver(opts)
	require.NoError(t, err)

	err = driver.Ping(ctx)
	require.NoError(t, err)

	// Clean up any existing test data
	err = driver.Drop(ctx, &TestObject{})
	if err != nil {
		// Ignore errors if table doesn't exist
		t.Logf("Drop table error (can be ignored for first run): %v", err)
	}

	// migrate to create the test table
	driver.Migrate(ctx, []model.DBObject{&TestObject{}}, model.DBM{})

	return driver, ctx
}

// teardownTest cleans up the test environment
func teardownTest(t *testing.T, driver *driver) {
	err := driver.Close()
	assert.NoError(t, err)
}
