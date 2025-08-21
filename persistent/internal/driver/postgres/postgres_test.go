//go:build postgres || postgres14 || postgres13 || postgres12 || postgres11 || postgres10
// +build postgres postgres14 postgres13 postgres12 postgres11 postgres10

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
	connStr = "host=localhost port=5432 user=postgres dbname=tyk password=secr3t sslmode=disable"

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

// TestInsert tests the Insert method
func TestInsert(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	obj := &TestObject{
		Name:      "Test Object",
		Value:     42,
		CreatedAt: time.Now(),
	}

	err := driver.Insert(ctx, obj)
	assert.NoError(t, err)
	assert.NotEmpty(t, obj.GetObjectID())

	var result TestObject
	err = driver.Query(ctx, obj, &result, model.DBM{"id": obj.GetObjectID()})
	assert.NoError(t, err)
	assert.Equal(t, obj.Name, result.Name)
	assert.Equal(t, obj.Value, result.Value)
}

func TestUpdate(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	obj := &TestObject{
		Name:      "Original Name",
		Value:     42,
		CreatedAt: time.Now(),
	}
	err := driver.Insert(ctx, obj)
	require.NoError(t, err)

	obj.Name = "Updated Name"
	obj.Value = 100
	err = driver.Update(ctx, obj)
	assert.NoError(t, err)

	var result TestObject
	err = driver.Query(ctx, obj, &result, model.DBM{"id": obj.GetObjectID()})
	assert.NoError(t, err)
	assert.Equal(t, "Updated Name", result.Name)
	assert.Equal(t, 100, result.Value)
}

func TestDelete(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// 1. Create and insert a test object
	obj := &TestObject{
		Name:      "To Be Deleted",
		Value:     42,
		CreatedAt: time.Now(),
	}

	err := driver.Insert(ctx, obj)
	require.NoError(t, err)
	require.NotEmpty(t, obj.GetObjectID(), "Object ID should be set after insert")

	// Verify the object was inserted
	var result TestObject
	err = driver.Query(ctx, obj, &result, model.DBM{"id": obj.GetObjectID()})
	require.NoError(t, err)
	assert.Equal(t, obj.Name, result.Name)
	assert.Equal(t, obj.Value, result.Value)

	// 2. Delete the object
	err = driver.Delete(ctx, obj)
	assert.NoError(t, err, "Delete operation should succeed")

	// 3. Verify the object was deleted
	var deletedResult TestObject
	err = driver.Query(ctx, obj, &deletedResult, model.DBM{"id": obj.GetObjectID()})
	assert.Error(t, err, "Query should return an error for deleted object")

	// 4. Test deleting with filter
	// Insert another object
	objWithFilter := &TestObject{
		Name:      "To Be Deleted With Filter",
		Value:     100,
		CreatedAt: time.Now(),
	}

	err = driver.Insert(ctx, objWithFilter)
	require.NoError(t, err)

	// Delete using a filter
	err = driver.Delete(ctx, objWithFilter, model.DBM{"value": 100})
	assert.NoError(t, err, "Delete with filter should succeed")

	// Verify deletion
	var filteredResult TestObject
	err = driver.Query(ctx, objWithFilter, &filteredResult, model.DBM{"id": objWithFilter.GetObjectID()})
	assert.Error(t, err, "Query should return an error for object deleted with filter")

	// 5. Test deleting non-existent object
	nonExistentObj := &TestObject{
		Name:  "Non-existent",
		Value: 999,
	}
	nonExistentObj.SetObjectID(model.NewObjectID())

	err = driver.Delete(ctx, nonExistentObj)
	// The behavior here depends on your driver implementation:
	// Some drivers return an error when no rows are affected, others don't
	// Adjust this assertion based on your expected behavior
	// assert.NoError(t, err, "Delete of non-existent object should not return an error")
	// OR
	// assert.Error(t, err, "Delete of non-existent object should return an error")

	// 6. Test deleting with invalid filter
	invalidObj := &TestObject{
		Name:  "Invalid Filter",
		Value: 200,
	}

	err = driver.Insert(ctx, invalidObj)
	require.NoError(t, err)

	// Use a filter that won't match any objects
	err = driver.Delete(ctx, invalidObj, model.DBM{"value": 999})
	// Again, adjust based on your expected behavior
	// assert.NoError(t, err, "Delete with non-matching filter should not return an error")
	// OR
	// assert.Error(t, err, "Delete with non-matching filter should return an error")

	// Verify the object still exists
	var stillExistsResult TestObject
	err = driver.Query(ctx, invalidObj, &stillExistsResult, model.DBM{"id": invalidObj.GetObjectID()})
	assert.NoError(t, err, "Object with non-matching delete filter should still exist")
	assert.Equal(t, invalidObj.Name, stillExistsResult.Name)
}
