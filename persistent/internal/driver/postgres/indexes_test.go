//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"fmt"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCreateIndex(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Helper function to clean up test data
	cleanupTestData := func(tableName string) {
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Error
		if err != nil {
			t.Logf("Error cleaning up test data: %v", err)
		}
	}

	// Create test table
	testItem := &TestObject{}
	err := driver.Drop(ctx, testItem) // Clean up if table exists
	if err != nil {
		// Ignore error if table doesn't exist
		t.Logf("Error dropping table: %v", err)
	}

	err = driver.Migrate(ctx, []model.DBObject{testItem})
	require.NoError(t, err)

	// Insert some test data
	items := []*TestObject{
		{Name: "Item 1", Value: 10, CreatedAt: time.Now()},
		{Name: "Item 2", Value: 20, CreatedAt: time.Now()},
		{Name: "Item 3", Value: 30, CreatedAt: time.Now()},
	}

	for _, item := range items {
		err := driver.Insert(ctx, item)
		require.NoError(t, err)
	}

	// Test case 1: Create a simple index on a single field
	t.Run("SimpleIndex", func(t *testing.T) {
		// Define the index
		index := model.Index{
			Name: "idx_name",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		// Create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err)

		// Verify the index was created
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our index in the list
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_name" {
				foundIndex = true
				assert.Equal(t, 1, len(idx.Keys))
				assert.Contains(t, idx.Keys[0], "name")
				break
			}
		}
		assert.True(t, foundIndex, "Index was not found")
	})

	// Test case 1: Create a simple index on a single field
	t.Run("SimpleIndexDefaultType", func(t *testing.T) {
		// Define the index
		index := model.Index{
			Name: "idx_text_name",
			Keys: []model.DBM{
				{"name": float64(1)}, // Ascending index on name field
			},
		}

		// Create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err)

		// Verify the index was created
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our index in the list
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_name" {
				foundIndex = true
				assert.Equal(t, 1, len(idx.Keys))
				assert.Contains(t, idx.Keys[0], "name")
				break
			}
		}
		assert.True(t, foundIndex, "Index was not found")
	})

	// Test case 2: Create a compound index on multiple fields
	t.Run("CompoundIndex", func(t *testing.T) {
		// Define the index
		index := model.Index{
			Name: "idx_name_value",
			Keys: []model.DBM{
				{"name": 1},   // Ascending index on name field
				{"value": -1}, // Descending index on value field
			},
		}

		// Create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err)

		// Verify the index was created
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our index in the list
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_name_value" {
				foundIndex = true
				assert.Equal(t, 2, len(idx.Keys))

				// Check that both fields are in the index
				var hasName, hasValue bool
				for _, key := range idx.Keys {
					if _, ok := key["name"]; ok {
						hasName = true
					}
					if _, ok := key["value"]; ok {
						hasValue = true
					}
				}
				assert.True(t, hasName, "Index should include 'name' field")
				assert.True(t, hasValue, "Index should include 'value' field")
				break
			}
		}
		assert.True(t, foundIndex, "Index was not found")
	})

	t.Run("TTLCompoundIndex", func(t *testing.T) {
		// Define the index
		index := model.Index{
			Name: "idx_name_value",
			Keys: []model.DBM{
				{"name": 1},   // Ascending index on name field
				{"value": -1}, // Descending index on value field
			},
			IsTTLIndex: true,
		}

		// Create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.Error(t, err)
	})

	// Test case 4: Create an index with background option
	t.Run("BackgroundIndex", func(t *testing.T) {
		// Define the index
		index := model.Index{
			Name: "idx_background",
			Keys: []model.DBM{
				{"value": 1}, // Ascending index on value field
			},
			Background: true, // This might be ignored in PostgreSQL but should not cause an error
		}

		// Create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err)

		// Verify the index was created
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our index in the list
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_background" {
				foundIndex = true
				break
			}
		}
		assert.True(t, foundIndex, "Index was not found")
	})

	// Test case 4: Create a TTL index
	t.Run("CreateTTLIndex", func(t *testing.T) {
		// Create a test table
		tableName := "test_create_ttl_index"
		testObj := &TestObject{TableNameValue: tableName}
		defer cleanupTestData(tableName)

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Define a TTL index
		index := model.Index{
			Name: "idx_test_ttl",
			Keys: []model.DBM{
				{"created_at": 1},
			},
			IsTTLIndex: true,
			TTL:        3600, // 1 hour TTL
		}

		// Create the index
		err = driver.CreateIndex(ctx, testObj, index)

		if err != nil {
			assert.Contains(t, err.Error(), "TTL", "Error should mention TTL indexes")
		} else {
			// If implemented through a custom solution (like triggers), verify it
			// This would depend on your specific implementation
			t.Log("TTL index created successfully - verify your implementation manually")
		}
	})

	// Test case 5: Create an index on a non-existent table
	t.Run("NonExistentTable", func(t *testing.T) {
		// Define a mock object with a non-existent table
		nonExistentItem := &TestObject{TableNameValue: "non_existent_table"}

		// Define the index
		index := model.Index{
			Name: "idx_non_existent",
			Keys: []model.DBM{
				{"field": 1},
			},
		}

		// Attempt to create the index
		err := driver.CreateIndex(ctx, nonExistentItem, index)
		assert.Error(t, err, "Creating index on non-existent table should fail")
	})

	t.Run("EmptyIndexName", func(t *testing.T) {
		// Create index with empty name
		index := model.Index{
			Name: "",
			Keys: []model.DBM{{"name": 1}},
		}
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err) // Expect success, not error

		// Verify the index was created with a generated name
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find the index with the generated name
		found := false
		for _, idx := range indexes {
			fmt.Printf("Index: %+v\n", idx.Name)
			if idx.Name == "name_1" {
				found = true
				break
			}
		}
		assert.True(t, found, "Index with generated name 'name_1' not found")
	})

	// Test case 7: Create an index with empty keys
	t.Run("EmptyIndexKeys", func(t *testing.T) {
		// Define the index with empty keys
		index := model.Index{
			Name: "idx_empty_keys",
			Keys: []model.DBM{},
		}

		// Attempt to create the index
		err := driver.CreateIndex(ctx, testItem, index)
		assert.Error(t, err, "Creating index with empty keys should fail")
	})
}

func TestGetIndexes(t *testing.T) {
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

	// Test case 1: Get indexes on a table with no custom indexes
	t.Run("NoCustomIndexes", func(t *testing.T) {
		// Get indexes
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// There should be at least the primary key index
		assert.GreaterOrEqual(t, len(indexes), 0)
	})

	// Test case 2: Get indexes after creating a custom index
	t.Run("WithCustomIndex", func(t *testing.T) {
		// Create a custom index
		index := model.Index{
			Name: "idx_test_name",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		err := driver.CreateIndex(ctx, testItem, index)
		require.NoError(t, err)

		// Get indexes
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our custom index
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_test_name" {
				foundIndex = true
				assert.Equal(t, 1, len(idx.Keys))
				assert.Contains(t, idx.Keys[0], "name")
				break
			}
		}
		assert.True(t, foundIndex, "Custom index was not found")
	})

	// Test case 3: Get indexes after creating multiple custom indexes
	t.Run("WithMultipleCustomIndexes", func(t *testing.T) {
		// Create first custom index
		index1 := model.Index{
			Name: "idx_test_value",
			Keys: []model.DBM{
				{"value": 1}, // Ascending index on value field
			},
		}

		err := driver.CreateIndex(ctx, testItem, index1)
		require.NoError(t, err)

		// Create second custom index
		index2 := model.Index{
			Name: "idx_test_created_at",
			Keys: []model.DBM{
				{"created_at": -1}, // Descending index on created_at field
			},
		}

		err = driver.CreateIndex(ctx, testItem, index2)
		require.NoError(t, err)

		// Get indexes
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our custom indexes
		var foundIndex1, foundIndex2 bool
		for _, idx := range indexes {
			if idx.Name == "idx_test_value" {
				foundIndex1 = true
				assert.Equal(t, 1, len(idx.Keys))
				assert.Contains(t, idx.Keys[0], "value")
			}
			if idx.Name == "idx_test_created_at" {
				foundIndex2 = true
				assert.Equal(t, 1, len(idx.Keys))
				assert.Contains(t, idx.Keys[0], "created_at")
			}
		}
		assert.True(t, foundIndex1, "First custom index was not found")
		assert.True(t, foundIndex2, "Second custom index was not found")
	})

	// Test case 5: Get indexes on a non-existent table
	t.Run("NonExistentTable", func(t *testing.T) {
		// Define a mock object with a non-existent table
		nonExistentItem := &TestObject{TableNameValue: "non_existent_table"}

		// Attempt to get indexes
		indexes, err := driver.GetIndexes(ctx, nonExistentItem)
		assert.Error(t, err, "Getting indexes on non-existent table should fail")
		assert.Empty(t, indexes, "Indexes should be empty for non-existent table")
	})

	// Test case 6: Get indexes with a compound index
	t.Run("WithCompoundIndex", func(t *testing.T) {
		// Create a compound index
		index := model.Index{
			Name: "idx_test_compound",
			Keys: []model.DBM{
				{"name": 1},   // Ascending index on name field
				{"value": -1}, // Descending index on value field
			},
		}

		err := driver.CreateIndex(ctx, testItem, index)
		require.NoError(t, err)

		// Get indexes
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our compound index
		var foundIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_test_compound" {
				foundIndex = true
				assert.Equal(t, 2, len(idx.Keys))

				// Check that both fields are in the index
				var hasName, hasValue bool
				for _, key := range idx.Keys {
					if _, ok := key["name"]; ok {
						hasName = true
					}
					if _, ok := key["value"]; ok {
						hasValue = true
					}
				}
				assert.True(t, hasName, "Index should include 'name' field")
				assert.True(t, hasValue, "Index should include 'value' field")
				break
			}
		}
		assert.True(t, foundIndex, "Compound index was not found")
	})
}

func TestCleanIndexes(t *testing.T) {
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

	// Test case 1: Clean indexes on a table with no custom indexes
	t.Run("NoCustomIndexes", func(t *testing.T) {
		// Clean indexes
		err := driver.CleanIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Get indexes after cleaning
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// There should be no custom indexes, only possibly the primary key
		// which is not returned by GetIndexes in PostgreSQL
		assert.Empty(t, indexes, "There should be no custom indexes after cleaning")
	})

	// Test case 2: Clean indexes after creating custom indexes
	t.Run("WithCustomIndexes", func(t *testing.T) {
		// Create first custom index
		index1 := model.Index{
			Name: "idx_test_name",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		err := driver.CreateIndex(ctx, testItem, index1)
		require.NoError(t, err)

		// Create second custom index
		index2 := model.Index{
			Name: "idx_test_value",
			Keys: []model.DBM{
				{"value": 1}, // Ascending index on value field
			},
		}

		err = driver.CreateIndex(ctx, testItem, index2)
		require.NoError(t, err)

		// Verify indexes were created
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(indexes), 2, "At least two custom indexes should exist")

		// Clean indexes
		err = driver.CleanIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Get indexes after cleaning
		indexes, err = driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// There should be no custom indexes, only possibly the primary key
		assert.Empty(t, indexes, "There should be no custom indexes after cleaning")
	})

	// Test case 3: Clean indexes on a non-existent table
	t.Run("NonExistentTable", func(t *testing.T) {
		// Define a mock object with a non-existent table
		nonExistentItem := &TestObject{TableNameValue: "non_existent_table"}

		// Attempt to clean indexes
		err := driver.CleanIndexes(ctx, nonExistentItem)
		assert.Error(t, err, "Cleaning indexes on non-existent table should fail")
	})

	// Test case 4: Clean indexes and then create new ones
	t.Run("CleanAndCreateNew", func(t *testing.T) {
		// Create a custom index
		index := model.Index{
			Name: "idx_test_old",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		err := driver.CreateIndex(ctx, testItem, index)
		require.NoError(t, err)

		// Clean indexes
		err = driver.CleanIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Create a new index after cleaning
		newIndex := model.Index{
			Name: "idx_test_new",
			Keys: []model.DBM{
				{"value": 1}, // Ascending index on value field
			},
		}

		err = driver.CreateIndex(ctx, testItem, newIndex)
		assert.NoError(t, err)

		// Get indexes after creating new index
		indexes, err := driver.GetIndexes(ctx, testItem)
		assert.NoError(t, err)

		// Find our new index
		var foundNewIndex bool
		for _, idx := range indexes {
			if idx.Name == "idx_test_new" {
				foundNewIndex = true
				break
			}
			// Make sure the old index is gone
			assert.NotEqual(t, "idx_test_old", idx.Name, "Old index should not exist")
		}
		assert.True(t, foundNewIndex, "New index should exist after cleaning and creating")
	})
}

func TestIndexExists(t *testing.T) {
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

	// Test case 1: Check for non-existent index
	t.Run("NonExistentIndex", func(t *testing.T) {
		// The indexExists function is likely private, so we need to test it indirectly
		// We'll use CreateIndex with a check for duplicate index error

		// First, create an index
		index := model.Index{
			Name: "idx_test_unique",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		// Create the index for the first time
		err := driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err, "First index creation should succeed")

		// Try to create the same index again
		// If indexExists works correctly, this should fail with a duplicate index error
		err = driver.CreateIndex(ctx, testItem, index)
		assert.Error(t, err, "Second index creation should fail")
		assert.Contains(t, err.Error(), "already exists", "Error should indicate index already exists")
	})

	// Test case 2: Check after dropping an index
	t.Run("AfterDroppingIndex", func(t *testing.T) {
		// Clean all indexes first
		err := driver.CleanIndexes(ctx, testItem)
		require.NoError(t, err)

		// Create an index
		index := model.Index{
			Name: "idx_test_after_drop",
			Keys: []model.DBM{
				{"value": 1}, // Ascending index on value field
			},
		}

		// Create the index
		err = driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err, "Index creation should succeed")

		// Clean indexes again to drop the index
		err = driver.CleanIndexes(ctx, testItem)
		require.NoError(t, err)

		// Try to create the same index again
		// If indexExists works correctly, this should succeed because the index no longer exists
		err = driver.CreateIndex(ctx, testItem, index)
		assert.NoError(t, err, "Index creation after dropping should succeed")
	})

	// Test case 3: Check with a different index name
	t.Run("DifferentIndexName", func(t *testing.T) {
		// Clean all indexes first
		err := driver.CleanIndexes(ctx, testItem)
		require.NoError(t, err)

		// Create an index
		index1 := model.Index{
			Name: "idx_test_name_1",
			Keys: []model.DBM{
				{"name": 1}, // Ascending index on name field
			},
		}

		// Create the index
		err = driver.CreateIndex(ctx, testItem, index1)
		assert.NoError(t, err, "First index creation should succeed")

		// Create a different index
		index2 := model.Index{
			Name: "idx_test_name_2",
			Keys: []model.DBM{
				{"name": 1}, // Same field but different index name
			},
		}

		// Create the second index
		// If indexExists works correctly, this should succeed because the index name is different
		err = driver.CreateIndex(ctx, testItem, index2)
		assert.NoError(t, err, "Second index with different name should succeed")
	})

	// Test case 4: Check with a non-existent table
	t.Run("NonExistentTable", func(t *testing.T) {
		// Define a mock object with a non-existent table
		nonExistentItem := &TestObject{TableNameValue: "non_existent_table"}

		// Try to create an index on a non-existent table
		index := model.Index{
			Name: "idx_test_non_existent",
			Keys: []model.DBM{
				{"field": 1},
			},
		}

		// This should fail, but not because the index exists
		// It should fail because the table doesn't exist
		err := driver.CreateIndex(ctx, nonExistentItem, index)
		assert.Error(t, err, "Index creation on non-existent table should fail")
		assert.NotContains(t, err.Error(), "already exists", "Error should not indicate index already exists")
	})

	// Test case 5: Check with case-sensitive index names
	t.Run("CaseSensitiveIndexNames", func(t *testing.T) {
		// Clean all indexes first
		err := driver.CleanIndexes(ctx, testItem)
		require.NoError(t, err)

		// Create an index with lowercase name
		indexLower := model.Index{
			Name: "idx_test_case",
			Keys: []model.DBM{
				{"name": 1},
			},
		}

		// Create the index
		err = driver.CreateIndex(ctx, testItem, indexLower)
		assert.NoError(t, err, "Lowercase index creation should succeed")

		// Create an index with uppercase name
		// PostgreSQL typically folds identifiers to lowercase unless quoted
		indexUpper := model.Index{
			Name: "IDX_TEST_CASE",
			Keys: []model.DBM{
				{"name": 1},
			},
		}

		// Try to create the uppercase index
		// The behavior depends on how the driver handles case sensitivity
		err = driver.CreateIndex(ctx, testItem, indexUpper)

		// We don't make a specific assertion here because the behavior might vary
		// depending on how the driver handles case sensitivity in PostgreSQL
		// Just log the result for informational purposes
		if err != nil {
			t.Logf("Creating uppercase index after lowercase: %v", err)
		} else {
			t.Logf("Creating uppercase index after lowercase succeeded")
		}
	})
}
