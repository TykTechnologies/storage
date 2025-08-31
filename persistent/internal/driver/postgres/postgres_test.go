//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

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

const connStr = "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"

type TestObject struct {
	ID             model.ObjectID `json:"id" gorm:"primaryKey"`
	Name           string         `json:"name"`
	Value          int            `json:"value"`
	CreatedAt      time.Time      `json:"created_at"`
	TableNameValue string         `json:"-"`
}

func (t *TestObject) TableName() string {
	if t.TableNameValue != "" {
		return t.TableNameValue
	}
	return "test_objects"
}

func (t *TestObject) GetObjectID() model.ObjectID {
	return t.ID
}

func (t *TestObject) SetObjectID(id model.ObjectID) {
	t.ID = id
}

type nullableTableName struct {
	TestObject
}

func (n *nullableTableName) TableName() string {
	return ""
}

func (n *nullableTableName) GetObjectID() model.ObjectID {
	return n.TestObject.GetObjectID()
}

func (n *nullableTableName) SetObjectID(id model.ObjectID) {
	n.TestObject.SetObjectID(id)
}

func setupTest(t *testing.T) (*driver, context.Context) {
	ctx := context.Background()

	dbDSN := connStr
	// Check for postgres_test_dsn environment variable
	if dsn := os.Getenv("postgres_test_dsn"); dsn != "" {
		dbDSN = dsn
	}

	// Use the same hardcoded values as in the Taskfile
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
