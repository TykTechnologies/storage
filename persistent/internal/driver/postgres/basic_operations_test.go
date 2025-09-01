//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"github.com/TykTechnologies/storage/persistent/model"
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

	var err error
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Create test table
	testItem := &TestObject{}
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
	})

	// Test case 2: BulkUpdate without filter (update by ID)
	t.Run("UpdateByID", func(t *testing.T) {
		// Insert test items
		items := []*TestObject{
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
			&TestObject{ID: items[0].ID, Name: "Updated Item 1", Value: 100},
		}

		err := driver.BulkUpdate(ctx, updatedItems)
		assert.NoError(t, err)

		// Verify updates
		result1 := &TestObject{}
		err = driver.Query(ctx, &TestObject{}, result1, model.DBM{"id": items[0].ID})
		assert.NoError(t, err)
		assert.Equal(t, "Updated Item 1", result1.Name)
		assert.Equal(t, 100, result1.Value)
	})

	// Test case 3: BulkUpdate with filter
	t.Run("UpdateWithFilter", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Insert test items
		items := []*TestObject{
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
		updateItems := []*TestObject{
			{Name: "Updated Category", Value: 100},
		}
		updateObjects := make([]model.DBObject, len(updateItems))
		for i, item := range updateItems {
			updateObjects[i] = item
		}

		err = driver.BulkUpdate(ctx, updateObjects, model.DBM{"name": "Category A"})
		assert.NoError(t, err)

		// Verify Category A items were updated
		var results []*TestObject
		err = driver.Query(ctx, &TestObject{}, &results, model.DBM{"name": "Updated Category"})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
	})
}

func TestUpdateAll(t *testing.T) {

	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Create test table
	testItem := &TestObject{}
	err := driver.Drop(ctx, testItem) // Clean up if table exists
	if err != nil {
		// Ignore error if table doesn't exist
		t.Logf("Error dropping table: %v", err)
	}

	err = driver.Migrate(ctx, []model.DBObject{testItem})
	require.NoError(t, err)

	// Test case 1: UpdateAll with empty update (should be no-op)
	t.Run("EmptyUpdate", func(t *testing.T) {
		// Insert a test item
		item := &TestObject{
			Name:      "Test Item",
			Value:     10,
			CreatedAt: time.Now(),
		}
		err := driver.Insert(ctx, item)
		require.NoError(t, err)

		// Empty update should be a no-op
		err = driver.UpdateAll(ctx, &TestObject{}, model.DBM{"id": item.ID}, model.DBM{})
		assert.NoError(t, err)

		// Verify item is unchanged
		result := &TestObject{}
		err = driver.Query(ctx, &TestObject{}, result, model.DBM{"id": item.ID})
		assert.NoError(t, err)
		assert.Equal(t, "Test Item", result.Name)
		assert.Equal(t, 10, result.Value)
	})

	// Test case 2: UpdateAll with $set operator
	t.Run("UpdateWithSet", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Insert test items
		items := []*TestObject{
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
		err = driver.UpdateAll(ctx, &TestObject{},
			model.DBM{"name": "Category A"},
			model.DBM{"$set": model.DBM{"name": "Updated Category", "value": 100}})
		assert.NoError(t, err)

		// Verify all Category A items were updated
		var results []*TestObject
		err = driver.Query(ctx, &TestObject{}, &results, model.DBM{"name": "Updated Category"})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))

		// All values should be updated to 100
		for _, result := range results {
			assert.Equal(t, 100, result.Value)
		}

		// Category B items should remain unchanged
		var resultsB []*TestObject
		err = driver.Query(ctx, &TestObject{}, &resultsB, model.DBM{"name": "Category B"})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resultsB))
		assert.Equal(t, 30, resultsB[0].Value)
	})

	// Test case 3: UpdateAll with $inc operator
	t.Run("UpdateWithInc", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Insert test items
		items := []*TestObject{
			{Name: "Item 1", Value: 10, CreatedAt: time.Now()},
			{Name: "Item 2", Value: 20, CreatedAt: time.Now()},
		}

		// Insert items
		for _, item := range items {
			err := driver.Insert(ctx, item)
			require.NoError(t, err)
		}

		// Increment all values by 5
		err = driver.UpdateAll(ctx, &TestObject{},
			model.DBM{}, // Empty query to match all items
			model.DBM{"$inc": model.DBM{"value": 5}})
		assert.NoError(t, err)

		// Verify all values were incremented
		var results []*TestObject
		err = driver.Query(ctx, &TestObject{}, &results, model.DBM{})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))

		// Find items by name to verify their values
		var item1, item2 *TestObject
		for _, result := range results {
			if result.Name == "Item 1" {
				item1 = result
			} else if result.Name == "Item 2" {
				item2 = result
			}
		}

		assert.NotNil(t, item1)
		assert.NotNil(t, item2)
		assert.Equal(t, 15, item1.Value) // 10 + 5
		assert.Equal(t, 25, item2.Value) // 20 + 5
	})

	// Test case 4: UpdateAll with multiple operators
	t.Run("UpdateWithMultipleOperators", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Insert test item
		item := &TestObject{
			Name:      "Test Item",
			Value:     10,
			CreatedAt: time.Now(),
		}
		err = driver.Insert(ctx, item)
		require.NoError(t, err)

		// Update with multiple operators
		err = driver.UpdateAll(ctx, &TestObject{},
			model.DBM{"id": item.ID},
			model.DBM{
				"$set": model.DBM{"name": "Updated Item"},
				"$inc": model.DBM{"value": 15},
			})
		assert.NoError(t, err)

		// Verify updates
		result := &TestObject{}
		err = driver.Query(ctx, &TestObject{}, result, model.DBM{"id": item.ID})
		assert.NoError(t, err)
		assert.Equal(t, "Updated Item", result.Name)
		assert.Equal(t, 25, result.Value) // 10 + 15
	})
}

func TestUpsert(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Create test table
	testItem := &TestObject{}
	err := driver.Drop(ctx, testItem) // Clean up if table exists
	if err != nil {
		// Ignore error if table doesn't exist
		t.Logf("Error dropping table: %v", err)
	}

	err = driver.Migrate(ctx, []model.DBObject{testItem})
	require.NoError(t, err)

	// Test case 1: Upsert with existing document (update)
	t.Run("UpsertExisting", func(t *testing.T) {
		// Insert a test item
		item := &TestObject{
			Name:      "Test Item",
			Value:     10,
			CreatedAt: time.Now(),
		}
		err := driver.Insert(ctx, item)
		require.NoError(t, err)

		// Create a row object to receive the result
		resultItem := &TestObject{}

		// Perform upsert - should update the existing document
		err = driver.Upsert(ctx, resultItem,
			model.DBM{"id": item.ID}, // Query to find the document
			model.DBM{"$set": model.DBM{"name": "Updated Item", "value": 20}}) // Update to apply
		assert.NoError(t, err)

		// Verify the document was updated
		assert.Equal(t, item.ID, resultItem.ID)
		assert.Equal(t, "Updated Item", resultItem.Name)
		assert.Equal(t, 20, resultItem.Value)

		// Double-check by querying
		queryResult := &TestObject{}
		err = driver.Query(ctx, &TestObject{}, queryResult, model.DBM{"id": item.ID})
		assert.NoError(t, err)
		assert.Equal(t, "Updated Item", queryResult.Name)
		assert.Equal(t, 20, queryResult.Value)
	})

	// Test case 2: Upsert with non-existing document (insert)
	t.Run("UpsertNonExisting", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Create a row object to receive the result
		resultItem := &TestObject{}

		// Perform upsert with a query that won't match any document
		err = driver.Upsert(ctx, resultItem,
			model.DBM{"name": "Non-Existent Item"}, // Query that won't match
			model.DBM{"$set": model.DBM{"name": "New Item", "value": 30}}) // Data to insert
		assert.NoError(t, err)

		// Verify a new document was inserted
		assert.NotEmpty(t, resultItem.ID) // Should have a new ID
		assert.Equal(t, "New Item", resultItem.Name)
		assert.Equal(t, 30, resultItem.Value)

		// Double-check by querying
		var queryResults []*TestObject
		err = driver.Query(ctx, &TestObject{}, &queryResults, model.DBM{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queryResults))
		assert.Equal(t, "New Item", queryResults[0].Name)
		assert.Equal(t, 30, queryResults[0].Value)
	})

	// Test case 3: Upsert with direct update (no $set operator)
	t.Run("UpsertDirectUpdate", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Insert a test item
		item := &TestObject{
			Name:      "Test Item",
			Value:     10,
			CreatedAt: time.Now(),
		}
		err = driver.Insert(ctx, item)
		require.NoError(t, err)

		// Create a row object to receive the result
		resultItem := &TestObject{}

		// Perform upsert with direct update (no $set operator)
		err = driver.Upsert(ctx, resultItem,
			model.DBM{"id": item.ID}, // Query to find the document
			model.DBM{"name": "Directly Updated", "value": 40}) // Direct update
		assert.NoError(t, err)

		// Verify the document was updated
		assert.Equal(t, item.ID, resultItem.ID)
		assert.Equal(t, "Directly Updated", resultItem.Name)
		assert.Equal(t, 40, resultItem.Value)
	})

	// Test case 4: Upsert with ID in query but not in update
	t.Run("UpsertPreserveID", func(t *testing.T) {
		// Clean up previous test data
		err := driver.Drop(ctx, &TestObject{})
		require.NoError(t, err)
		err = driver.Migrate(ctx, []model.DBObject{&TestObject{}})
		require.NoError(t, err)

		// Generate a specific ID to use in the query
		specificID := model.NewObjectID()

		// Create a row object to receive the result
		resultItem := &TestObject{}
		resultItem.SetObjectID(specificID)

		// Perform upsert with ID in query
		err = driver.Upsert(ctx, resultItem,
			model.DBM{"id": specificID}, // Query with specific ID
			model.DBM{"$set": model.DBM{"name": "ID Preserved", "value": 50}}) // Update without ID
		assert.NoError(t, err)

		// Verify the document was inserted with the ID from the query
		assert.Equal(t, specificID, resultItem.ID)
		assert.Equal(t, "ID Preserved", resultItem.Name)
		assert.Equal(t, 50, resultItem.Value)
	})
}
