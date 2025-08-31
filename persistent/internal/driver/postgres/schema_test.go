//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/TykTechnologies/storage/persistent/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestHasTable(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Create test table
	testItem := &TestObject{}
	err := driver.Drop(ctx, testItem) // Clean up if table exists
	if err != nil {
		// Ignore error if table doesn't exist
		t.Logf("Error dropping table: %v", err)
	}

	// Test case 1: Table doesn't exist yet
	t.Run("TableDoesNotExist", func(t *testing.T) {
		exists, err := driver.HasTable(ctx, testItem.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table should not exist yet")
	})

	// Create the table
	err = driver.Migrate(ctx, []model.DBObject{testItem})
	require.NoError(t, err)

	// Test case 2: Table exists
	t.Run("TableExists", func(t *testing.T) {
		exists, err := driver.HasTable(ctx, testItem.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Table should exist after migration")
	})

	// Test case 3: Empty table name
	t.Run("EmptyTableName", func(t *testing.T) {
		exists, err := driver.HasTable(ctx, "")
		assert.Error(t, err)
		assert.False(t, exists, "Should return error for empty table name")
	})

	// Test case 4: Table with special characters in name
	t.Run("SpecialCharactersInTableName", func(t *testing.T) {
		specialNameItem := &TestObject{TableNameValue: "test-table-with-hyphens"}

		// First ensure the table doesn't exist
		exists, err := driver.HasTable(ctx, specialNameItem.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Special character table should not exist yet")

		// Create the table
		err = driver.Migrate(ctx, []model.DBObject{specialNameItem})
		require.NoError(t, err)

		// Now check if it exists
		exists, err = driver.HasTable(ctx, specialNameItem.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Special character table should exist after migration")

		// Clean up
		err = driver.Drop(ctx, specialNameItem)
		assert.NoError(t, err)
	})

	// Test case 5: Different database schema
	t.Run("DifferentSchema", func(t *testing.T) {
		// This test assumes the driver supports specifying schemas
		// If not, you may need to modify or remove this test

		// Create a schema if it doesn't exist
		err := driver.db.WithContext(ctx).Exec("CREATE SCHEMA IF NOT EXISTS test_schema").Error
		require.NoError(t, err)

		schemaItem := &TestObject{TableNameValue: "test_schema.schema_table"}

		// Check if table exists (should not)
		exists, err := driver.HasTable(ctx, schemaItem.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Schema table should not exist yet")

		// Create the table in the schema
		err = driver.Migrate(ctx, []model.DBObject{schemaItem})
		require.NoError(t, err)

		// Check if table exists now
		exists, err = driver.HasTable(ctx, schemaItem.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Schema table should exist after migration")

		// Clean up
		err = driver.Drop(ctx, schemaItem)
		assert.NoError(t, err)

		// Drop the schema
		err = driver.db.WithContext(ctx).Exec("DROP SCHEMA IF EXISTS test_schema CASCADE").Error
		assert.NoError(t, err)
	})
}

func TestGetTables(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Clean up any existing test tables
	cleanupTestTables(t, driver, ctx)

	// Test case 1: Empty database (or at least no test tables)
	t.Run("EmptyDatabase", func(t *testing.T) {
		tables, err := driver.GetTables(ctx)
		assert.NoError(t, err)

		// There might be system tables, so we'll check that our test tables aren't there
		for _, table := range tables {
			assert.NotEqual(t, "test_objects", table, "test_objects table should not exist yet")
			assert.NotEqual(t, "test_users", table, "test_users table should not exist yet")
			assert.NotEqual(t, "test_products", table, "test_products table should not exist yet")
		}
	})

	// Create test tables
	testTables := []model.DBObject{
		&TestObject{},
		&TestObject{TableNameValue: "test_users"},
		&TestObject{TableNameValue: "test_products"},
	}

	for _, obj := range testTables {
		err := driver.Migrate(ctx, []model.DBObject{obj})
		require.NoError(t, err)
	}

	// Test case 2: Multiple tables
	t.Run("MultipleTables", func(t *testing.T) {
		tables, err := driver.GetTables(ctx)
		assert.NoError(t, err)

		// Check that our test tables are in the list
		foundTables := make(map[string]bool)
		for _, table := range tables {
			if table == "test_objects" || table == "test_users" || table == "test_products" {
				foundTables[table] = true
			}
		}

		assert.True(t, foundTables["test_objects"], "test_objects table should exist")
		assert.True(t, foundTables["test_users"], "test_users table should exist")
		assert.True(t, foundTables["test_products"], "test_products table should exist")
	})

	// Test case 3: Table with special characters in name
	t.Run("TableWithSpecialCharacters", func(t *testing.T) {
		// Create a table with special characters in the name
		// Note: PostgreSQL will quote these names automatically
		specialTable := &TestObject{TableNameValue: "test_special_chars$"}
		err := driver.Migrate(ctx, []model.DBObject{specialTable})
		require.NoError(t, err)

		// Get all tables
		tables, err := driver.GetTables(ctx)
		assert.NoError(t, err)

		// Check if the special table is included
		foundSpecialTable := false
		for _, table := range tables {
			if table == "test_special_chars$" {
				foundSpecialTable = true
				break
			}
		}

		assert.True(t, foundSpecialTable, "Table with special characters should be included in results")

		// Clean up
		err = driver.Drop(ctx, specialTable)
		assert.NoError(t, err)
	})

	// Clean up the test tables
	cleanupTestTables(t, driver, ctx)
}

func TestDropTable(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Test case 1: Drop a non-existent table
	t.Run("DropNonExistentTable", func(t *testing.T) {
		_, err := driver.DropTable(ctx, "non_existent_table")
		// The behavior here depends on your implementation:
		// Option 1: Return an error when the table doesn't exist
		// assert.Error(t, err, "Dropping a non-existent table should return an error")

		// Option 2: Silently ignore if the table doesn't exist (IF EXISTS behavior)
		assert.NoError(t, err, "Dropping a non-existent table should not return an error when using IF EXISTS")
	})

	// Test case 2: Drop an existing table
	t.Run("DropExistingTable", func(t *testing.T) {
		// Create a test table
		testObj := &TestObject{TableNameValue: "test_drop_table"}
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, testObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table should exist after migration")

		// Drop the table
		_, err = driver.DropTable(ctx, "test_drop_table")
		assert.NoError(t, err, "Dropping an existing table should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, testObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table should not exist after being dropped")
	})

	// Test case 3: Drop a table with special characters in the name
	t.Run("DropTableWithSpecialCharacters", func(t *testing.T) {
		// Create a test table with special characters
		specialObj := &TestObject{TableNameValue: "test_special$chars"}
		err := driver.Migrate(ctx, []model.DBObject{specialObj})
		require.NoError(t, err, "Failed to create test table with special characters")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, specialObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table with special characters should exist after migration")

		// Drop the table
		_, err = driver.DropTable(ctx, "test_special$chars")
		assert.NoError(t, err, "Dropping a table with special characters should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, specialObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table with special characters should not exist after being dropped")
	})

	// Test case 4: Drop a table in a different schema
	t.Run("DropTableInDifferentSchema", func(t *testing.T) {
		// Create a test schema
		err := driver.db.WithContext(ctx).Exec("CREATE SCHEMA IF NOT EXISTS test_schema").Error
		require.NoError(t, err, "Failed to create test schema")

		// Create a test table in the schema
		schemaObj := &TestObject{TableNameValue: "test_schema.schema_table"}
		err = driver.Migrate(ctx, []model.DBObject{schemaObj})
		require.NoError(t, err, "Failed to create test table in schema")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, schemaObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table in schema should exist after migration")

		// Drop the table with schema prefix
		_, err = driver.DropTable(ctx, "test_schema.schema_table")
		assert.NoError(t, err, "Dropping a table with schema prefix should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, schemaObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table in schema should not exist after being dropped")

		// Drop the schema
		err = driver.db.WithContext(ctx).Exec("DROP SCHEMA IF EXISTS test_schema CASCADE").Error
		assert.NoError(t, err, "Failed to drop test schema")
	})

	// Test case 5: Drop a table with empty name
	t.Run("DropTableWithEmptyName", func(t *testing.T) {
		_, err := driver.DropTable(ctx, "")
		assert.Error(t, err, "Dropping a table with empty name should return an error")
		assert.Contains(t, err.Error(), "empty", "Error message should mention empty table name")
	})

	// Test case 6: Drop a table with very long name
	t.Run("DropTableWithLongName", func(t *testing.T) {
		// PostgreSQL has a limit of 63 bytes for identifiers
		longName := strings.Repeat("a", 63)

		// Create a test table with a long name
		longObj := &TestObject{TableNameValue: longName}
		err := driver.Migrate(ctx, []model.DBObject{longObj})
		require.NoError(t, err, "Failed to create test table with long name")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, longObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table with long name should exist after migration")

		// Drop the table
		_, err = driver.DropTable(ctx, longName)
		assert.NoError(t, err, "Dropping a table with long name should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, longObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table with long name should not exist after being dropped")
	})

	// Test case 7: Drop multiple tables in sequence
	t.Run("DropMultipleTables", func(t *testing.T) {
		// Create multiple test tables
		tableNames := []string{"multi_table1", "multi_table2", "multi_table3"}
		for _, name := range tableNames {
			obj := &TestObject{TableNameValue: name}
			err := driver.Migrate(ctx, []model.DBObject{obj})
			require.NoError(t, err, "Failed to create test table "+name)
		}

		// Drop each table and verify it's gone
		for _, name := range tableNames {
			obj := &TestObject{TableNameValue: name}

			// Verify the table exists before dropping
			exists, err := driver.HasTable(ctx, obj.TableName())
			require.NoError(t, err)
			require.True(t, exists, "Table "+name+" should exist before dropping")

			// Drop the table
			_, err = driver.DropTable(ctx, name)
			assert.NoError(t, err, "Dropping table "+name+" should not return an error")

			// Verify the table no longer exists
			exists, err = driver.HasTable(ctx, obj.TableName())
			assert.NoError(t, err)
			assert.False(t, exists, "Table "+name+" should not exist after being dropped")
		}
	})
}

func TestDrop(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Test case 1: Drop a non-existent table
	t.Run("DropNonExistentTable", func(t *testing.T) {
		nonExistentObj := &TestObject{TableNameValue: "non_existent_table"}
		err := driver.Drop(ctx, nonExistentObj)
		// The behavior here depends on your implementation:
		// Option 1: Return an error when the table doesn't exist
		// assert.Error(t, err, "Dropping a non-existent table should return an error")

		// Option 2: Silently ignore if the table doesn't exist (IF EXISTS behavior)
		assert.NoError(t, err, "Dropping a non-existent table should not return an error when using IF EXISTS")
	})

	// Test case 2: Drop an existing table
	t.Run("DropExistingTable", func(t *testing.T) {
		// Create a test table
		testObj := &TestObject{TableNameValue: "test_drop_object"}
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, testObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table should exist after migration")
		require.NoError(t, err)
		require.True(t, exists, "Table should exist after migration")
		require.NoError(t, err)
		require.True(t, exists, "Table should exist after migration")

		// Drop the table
		err = driver.Drop(ctx, testObj)
		assert.NoError(t, err, "Dropping an existing table should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, testObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table should not exist after being dropped")
	})

	// Test case 3: Drop with nil object
	t.Run("DropWithNilObject", func(t *testing.T) {
		err := driver.Drop(ctx, nil)
		assert.Error(t, err, "Dropping with nil object should return an error")
		assert.Contains(t, err.Error(), "nil", "Error message should mention nil object")
	})

	// Test case 4: Drop with object that has empty table name
	t.Run("DropWithEmptyTableName", func(t *testing.T) {
		emptyNameObj := &nullableTableName{}
		err := driver.Drop(ctx, emptyNameObj)
		assert.Error(t, err, "Dropping with empty table name should return an error")
		assert.Contains(t, err.Error(), "empty", "Error message should mention empty table name")
	})

	// Test case 5: Drop a table with special characters in the name
	t.Run("DropTableWithSpecialCharacters", func(t *testing.T) {
		// Create a test table with special characters
		specialObj := &TestObject{TableNameValue: "test_special$chars"}
		err := driver.Migrate(ctx, []model.DBObject{specialObj})
		require.NoError(t, err, "Failed to create test table with special characters")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, specialObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table with special characters should exist after migration")

		// Drop the table
		err = driver.Drop(ctx, specialObj)
		assert.NoError(t, err, "Dropping a table with special characters should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, specialObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table with special characters should not exist after being dropped")
	})

	// Test case 6: Drop a table with case-sensitive name
	t.Run("DropTableWithCaseSensitiveName", func(t *testing.T) {
		// Create a test table with uppercase name
		// PostgreSQL will convert this to lowercase unless quoted
		upperObj := &TestObject{TableNameValue: "TEST_UPPERCASE"}
		err := driver.Migrate(ctx, []model.DBObject{upperObj})
		require.NoError(t, err, "Failed to create test table with uppercase name")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, upperObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table with uppercase name should exist after migration")

		// Drop the table using the same object
		err = driver.Drop(ctx, upperObj)
		assert.NoError(t, err, "Dropping a table with uppercase name should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, upperObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table should not exist after being dropped")
	})

	// Test case 7: Drop a table in a different schema
	t.Run("DropTableInDifferentSchema", func(t *testing.T) {
		// Create a test schema
		err := driver.db.WithContext(ctx).Exec("CREATE SCHEMA IF NOT EXISTS test_schema").Error
		require.NoError(t, err, "Failed to create test schema")

		// Create a test table in the schema
		schemaObj := &TestObject{TableNameValue: "test_schema.schema_table"}
		err = driver.Migrate(ctx, []model.DBObject{schemaObj})
		require.NoError(t, err, "Failed to create test table in schema")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, schemaObj.TableName())
		require.NoError(t, err)
		require.True(t, exists, "Table in schema should exist after migration")

		// Drop the table
		err = driver.Drop(ctx, schemaObj)
		assert.NoError(t, err, "Dropping a table in a schema should not return an error")

		// Verify the table no longer exists
		exists, err = driver.HasTable(ctx, schemaObj.TableName())
		assert.NoError(t, err)
		assert.False(t, exists, "Table in schema should not exist after being dropped")

		// Drop the schema
		err = driver.db.WithContext(ctx).Exec("DROP SCHEMA IF EXISTS test_schema CASCADE").Error
		assert.NoError(t, err, "Failed to drop test schema")
	})
}

// Helper function to clean up test tables
func cleanupTestTables(t *testing.T, driver *driver, ctx context.Context) {
	tables := []model.DBObject{
		&TestObject{},
		&TestObject{TableNameValue: "test_users"},
		&TestObject{TableNameValue: "test_products"},
	}

	for _, obj := range tables {
		err := driver.Drop(ctx, obj)
		if err != nil {
			// Ignore errors if tables don't exist
			t.Logf("Error dropping table %s: %v", obj.TableName(), err)
		}
	}
}

func TestMigrate(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Helper function to clean up test tables
	cleanupTables := func(objects []model.DBObject) {
		for _, obj := range objects {
			if obj != nil {
				err := driver.Drop(ctx, obj)
				if err != nil {
					t.Logf("Error dropping table %s: %v", obj.TableName(), err)
				}
			}
		}
	}

	// Test case 1: Migrate a single new table
	t.Run("MigrateSingleNewTable", func(t *testing.T) {
		testObj := &TestObject{TableNameValue: "test_migrate_single"}
		defer cleanupTables([]model.DBObject{testObj})

		// Verify the table doesn't exist yet
		exists, err := driver.HasTable(ctx, testObj.TableName())
		require.NoError(t, err)
		require.False(t, exists, "Table should not exist before migration")

		// Migrate the table
		err = driver.Migrate(ctx, []model.DBObject{testObj})
		assert.NoError(t, err, "Migrating a new table should not return an error")

		// Verify the table now exists
		exists, err = driver.HasTable(ctx, testObj.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Table should exist after migration")
	})

	// Test case 2: Migrate multiple new tables
	t.Run("MigrateMultipleNewTables", func(t *testing.T) {
		testObjs := []model.DBObject{
			&TestObject{TableNameValue: "test_migrate_multi1"},
			&TestObject{TableNameValue: "test_migrate_multi2"},
			&TestObject{TableNameValue: "test_migrate_multi3"},
		}
		defer cleanupTables(testObjs)

		// Verify the tables don't exist yet
		for _, obj := range testObjs {
			exists, err := driver.HasTable(ctx, obj.TableName())
			require.NoError(t, err)
			require.False(t, exists, "Table "+obj.TableName()+" should not exist before migration")
		}

		// Migrate the tables
		err := driver.Migrate(ctx, testObjs)
		assert.NoError(t, err, "Migrating multiple new tables should not return an error")

		// Verify the tables now exist
		for _, obj := range testObjs {
			exists, err := driver.HasTable(ctx, obj.TableName())
			assert.NoError(t, err)
			assert.True(t, exists, "Table "+obj.TableName()+" should exist after migration")
		}
	})

	// Test case 3: Migrate an existing table (should not error)
	t.Run("MigrateExistingTable", func(t *testing.T) {
		testObj := &TestObject{TableNameValue: "test_migrate_existing"}
		defer cleanupTables([]model.DBObject{testObj})

		// Create the table first
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "First migration should succeed")

		// Migrate the table again
		err = driver.Migrate(ctx, []model.DBObject{testObj})
		assert.NoError(t, err, "Migrating an existing table should not return an error")

		// Verify the table still exists
		exists, err := driver.HasTable(ctx, testObj.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Table should still exist after second migration")
	})

	// Test case 6: Migrate with nil object in slice
	t.Run("MigrateWithNilObject", func(t *testing.T) {
		testObjs := []model.DBObject{
			&TestObject{TableNameValue: "test_migrate_with_nil"},
			nil,
		}
		defer cleanupTables(testObjs)

		err := driver.Migrate(ctx, testObjs)
		assert.Error(t, err, "Migrating with a nil object should return an error")
		assert.Contains(t, err.Error(), "nil", "Error message should mention nil object")
	})

	// Test case 6: Migrate with object that has empty table name
	t.Run("MigrateWithEmptyTableName", func(t *testing.T) {
		testObjs := []model.DBObject{
			&TestObject{TableNameValue: "test_migrate_with_empty"},
			&nullableTableName{},
		}
		defer cleanupTables(testObjs)

		err := driver.Migrate(ctx, testObjs)
		assert.Error(t, err, "Migrating with an object that has empty table name should return an error")
		assert.Contains(t, err.Error(), "empty", "Error message should mention empty table name")
	})

	// Test case 7: Migrate with options
	t.Run("MigrateWithOptions", func(t *testing.T) {
		testObj := &TestObject{TableNameValue: "test_migrate_with_options"}
		defer cleanupTables([]model.DBObject{testObj})

		// Define some options
		options := model.DBM{
			"capped":   true,
			"maxBytes": 1024 * 1024, // 1MB
		}

		// Migrate with options
		err := driver.Migrate(ctx, []model.DBObject{testObj}, options)
		assert.NoError(t, err, "Migrating with options should not return an error")

		// Verify the table exists
		exists, err := driver.HasTable(ctx, testObj.TableName())
		assert.NoError(t, err)
		assert.True(t, exists, "Table should exist after migration with options")
	})
}

func TestGetDatabaseInfo(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Test case 1: Successfully get database info
	t.Run("SuccessfullyGetDatabaseInfo", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// Check that the returned info is not empty
		assert.NotEmpty(t, info.Name, "Database name should not be empty")
		assert.NotEmpty(t, info.Version, "Database version should not be empty")
		assert.NotEmpty(t, info.FullVersion, "Full database version should not be empty")

		// Check that the database type is PostgreSQL
		assert.Equal(t, utils.PostgresDB, info.Type, "Database type should be PostgreSQL")

		// Log the database info for debugging
		t.Logf("Database Info: %+v", info)
	})

	// Test case 2: Check specific PostgreSQL version format
	t.Run("CheckPostgreSQLVersionFormat", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// PostgreSQL version typically follows the format X.Y or X.Y.Z
		// Use a regex to validate the format
		versionPattern := regexp.MustCompile(`^\d+\.\d+(\.\d+)?`)
		assert.True(t, versionPattern.MatchString(info.Version),
			"PostgreSQL version should match format X.Y or X.Y.Z, got: %s", info.Version)
	})

	// Test case 3: Check database name matches expected
	t.Run("CheckDatabaseName", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// The database name should match what we expect from the connection string
		// This assumes the connection string in setupTest uses "tyk" as the database name
		assert.Equal(t, "tyk", info.Name, "Database name should match the one in connection string")
	})

	// Test case 4: Check user matches expected
	t.Run("CheckDatabaseUser", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// The user should match what we expect from the connection string
		// This assumes the connection string in setupTest uses "postgres" as the user
		assert.Equal(t, "postgres", info.User, "Database user should match the one in connection string")
	})

	// Test case 5: Check database size
	t.Run("CheckDatabaseSize", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// The database size should be a positive number
		assert.GreaterOrEqual(t, info.SizeBytes, int64(0), "Database size should be non-negative")

		t.Logf("Database size: %d bytes", info.SizeBytes)
	})

	// Test case 6: Check connection counts
	t.Run("CheckConnectionCounts", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// Max connections should be a positive number
		assert.Greater(t, info.MaxConnections, 0, "Max connections should be positive")

		// Current connections should be a non-negative number
		assert.GreaterOrEqual(t, info.CurrentConnections, 0, "Current connections should be non-negative")

		// Current connections should not exceed max connections
		assert.LessOrEqual(t, info.CurrentConnections, info.MaxConnections,
			"Current connections should not exceed max connections")

		t.Logf("Connections: %d/%d", info.CurrentConnections, info.MaxConnections)
	})

	// Test case 7: Check table count
	t.Run("CheckTableCount", func(t *testing.T) {
		// Create a few test tables
		testObjs := []model.DBObject{
			&TestObject{TableNameValue: "test_info_table1"},
			&TestObject{TableNameValue: "test_info_table2"},
			&TestObject{TableNameValue: "test_info_table3"},
		}

		// Clean up tables after the test
		defer func() {
			for _, obj := range testObjs {
				driver.Drop(ctx, obj)
			}
		}()

		// Create the tables
		err := driver.Migrate(ctx, testObjs)
		require.NoError(t, err, "Creating test tables should not return an error")

		// Get database info
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// Table count should be a non-negative number
		assert.GreaterOrEqual(t, info.TableCount, 0, "Table count should be non-negative")

		// Table count should include at least our test tables
		assert.GreaterOrEqual(t, info.TableCount, 3, "Table count should include at least our test tables")

		t.Logf("Table count: %d", info.TableCount)
	})

	// Test case 8: Check start time
	t.Run("CheckStartTime", func(t *testing.T) {
		info, err := driver.GetDatabaseInfo(ctx)
		assert.NoError(t, err, "Getting database info should not return an error")

		// Start time should be in the past
		assert.True(t, info.StartTime.Before(time.Now()), "Database start time should be in the past")

		// Start time should not be the zero value
		assert.False(t, info.StartTime.IsZero(), "Database start time should not be zero")

		t.Logf("Database start time: %v", info.StartTime)
	})
}

func TestDBTableStats(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Test case 1: Get stats for a table with data
	t.Run("GetStatsForTableWithData", func(t *testing.T) {
		// Create a test table
		testObj := &TestObject{TableNameValue: "test_stats_table"}
		defer driver.Drop(ctx, testObj)

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Creating test table should not return an error")

		// Insert some test data
		for i := 0; i < 10; i++ {
			obj := &TestObject{
				TableNameValue: "test_stats_table",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Inserting test data should not return an error")
		}

		// Get table stats
		stats, err := driver.DBTableStats(ctx, testObj)
		assert.NoError(t, err, "Getting table stats should not return an error")

		// Check that stats are not empty
		assert.NotEmpty(t, stats, "Table stats should not be empty")

		// Log the stats for debugging
		t.Logf("Table stats: %+v", stats)

		// Check for expected stats fields
		// The exact fields may vary depending on your implementation, but these are common ones
		assert.Contains(t, stats, "size_bytes", "Stats should include size in bytes")

		// Check that size is positive
		if sizeBytes, ok := stats["size_bytes"].(int64); ok {
			assert.Greater(t, sizeBytes, int64(0), "Size in bytes should be positive")
		} else if sizeBytes, ok := stats["size_bytes"].(float64); ok {
			assert.Greater(t, sizeBytes, float64(0), "Size in bytes should be positive")
		} else {
			t.Errorf("size_bytes is not of expected type, got %T", stats["size_bytes"])
		}
	})

	// Test case 2: Get stats for an empty table
	t.Run("GetStatsForEmptyTable", func(t *testing.T) {
		// Create a test table
		emptyObj := &TestObject{TableNameValue: "test_empty_stats_table"}
		defer driver.Drop(ctx, emptyObj)

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{emptyObj})
		require.NoError(t, err, "Creating empty test table should not return an error")

		// Get table stats
		stats, err := driver.DBTableStats(ctx, emptyObj)
		assert.NoError(t, err, "Getting stats for empty table should not return an error")

		// Check that stats are not empty
		assert.NotEmpty(t, stats, "Table stats should not be empty")

		// Check that row count is zero
		if rowCount, ok := stats["row_count"].(int64); ok {
			assert.Equal(t, int64(0), rowCount, "Row count should be zero for empty table")
		} else if rowCount, ok := stats["row_count"].(float64); ok {
			assert.Equal(t, float64(0), rowCount, "Row count should be zero for empty table")
		} else {
			t.Errorf("row_count is not of expected type, got %T", stats["row_count"])
		}
	})

	// Test case 3: Get stats for a non-existent table
	t.Run("GetStatsForNonExistentTable", func(t *testing.T) {
		nonExistentObj := &TestObject{TableNameValue: "non_existent_table"}

		// Get table stats
		_, err := driver.DBTableStats(ctx, nonExistentObj)
		assert.Error(t, err, "Getting stats for non-existent table should return an error")
	})

	// Test case 4: Get stats with nil object
	t.Run("GetStatsWithNilObject", func(t *testing.T) {
		stats, err := driver.DBTableStats(ctx, nil)
		assert.Error(t, err, "Getting stats with nil object should return an error")
		assert.Empty(t, stats, "Stats should be empty when object is nil")
	})

	// Test case 5: Get stats with object that has empty table name
	t.Run("GetStatsWithEmptyTableName", func(t *testing.T) {
		emptyNameObj := &nullableTableName{}

		stats, err := driver.DBTableStats(ctx, emptyNameObj)
		assert.Error(t, err, "Getting stats with empty table name should return an error")
		assert.Empty(t, stats, "Stats should be empty when table name is empty")
	})
}
