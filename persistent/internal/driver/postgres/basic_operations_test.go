//go:build postgres || postgres14 || postgres13 || postgres12.22 || postgres11 || postgres10
// +build postgres postgres14 postgres13 postgres12.22 postgres11 postgres10

package postgres

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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

func TestBulkUpdate(t *testing.T) {

	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)
	
	// Create test table
	testItem := &TestItem{}
	err = driver.Drop(ctx, testItem) // Clean up if table exists
	if err != nil {
		// Ignore error if table doesn't exist
		t.Logf("Error dropping table: %v", err)
	}

	err = driver.Migrate(ctx, []model.DBObject{testItem})
	require.NoError(t, err)

	// Test case 1: BulkUpdate with empty objects
	t.Run("EmptyObjects", func(t *testing.T) {
		err := driver.BulkUpdate(ctx, []model.DBObject{})
		assert.Error(t, err)
		assert.Equal(t, types.ErrorEmptyRow, err.Error())
	})

	// Test case 2: BulkUpdate without filter (update by ID)
	t.Run("UpdateByID", func(t *testing.T) {
		// Insert test items
		items := []*TestItem{
			{Name: "Item 1", Value: 10, CreatedAt: time.Now()},
			{Name: "Item 2", Value: 20, CreatedAt: time.Now()},
		}

		// Insert items
		for _, item := range items {
			err := driver.Insert(ctx, item)
			require.NoError(t, err)
		}

		// Update items
		updatedItems := []model.DBObject{
			&TestItem{ID: items[0].ID, Name: "Updated Item 1", Value: 100},
		}

		err := driver.BulkUpdate(ctx, updatedItems)
		assert.NoError(t, err)

		// Verify updates
		result1 := &TestItem{}
		err = driver.Query(ctx, &TestItem{}, result1, model.DBM{"_id": items[0].ID})
		assert.NoError(t, err)
		assert.Equal(t, "Updated Item 1", result1.Name)
		assert.Equal(t, 100, result1.Value)
	})

	// Test case 3: BulkUpdate with filter
	t.Run("UpdateWithFilter", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestItem{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestItem{}})
		require.NoError(t, err)

		// Insert test items
		items := []*TestItem{
			{Name: "Category A", Value: 10, CreatedAt: time.Now()},
			{Name: "Category A", Value: 20, CreatedAt: time.Now()},
			{Name: "Category B", Value: 30, CreatedAt: time.Now()},
		}

		// Insert items
		for _, item := range items {
			err := driver.Insert(ctx, item)
			require.NoError(t, err)
		}

		// Update all Category A items
		updateItems := []*TestItem{
			{Name: "Updated Category", Value: 100},
		}
		updateObjects := make([]model.DBObject, len(updateItems))
		for i, item := range updateItems {
			updateObjects[i] = item
		}

		err = driver.BulkUpdate(ctx, updateObjects, model.DBM{"name": "Category A"})
		assert.NoError(t, err)

		// Verify Category A items were updated
		var results []*TestItem
		err = driver.Query(ctx, &TestItem{}, &results, model.DBM{"name": "Updated Category"})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
	})
}
