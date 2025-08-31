//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func getConnStr() string {
	dbDSN := connStr
	// Check for postgres_test_dsn environment variable
	if dsn := os.Getenv("postgres_test_dsn"); dsn != "" {
		dbDSN = dsn
	}
	return dbDSN
}

func TestNewPostgresDriver(t *testing.T) {
	// Test case 1: Successful connection
	t.Run("SuccessfulConnection", func(t *testing.T) {
		// Create client options with valid connection string
		opts := &types.ClientOpts{
			ConnectionString: getConnStr(),
			Type:             "postgres",
		}

		// Create a new driver
		driver, err := NewPostgresDriver(opts)

		// Assert no error and driver is not nil
		assert.NoError(t, err)
		assert.NotNil(t, driver)

		// Verify the driver is connected by pinging the database
		err = driver.Ping(context.Background())
		assert.NoError(t, err)

		// Clean up
		err = driver.Close()
		assert.NoError(t, err)
	})

	// Test case 2: Invalid connection string
	t.Run("InvalidConnectionString", func(t *testing.T) {
		// Create client options with invalid connection string
		opts := &types.ClientOpts{
			ConnectionString: "postgres://invalid:invalid@nonexistent:5432/nonexistent",
			Type:             "postgres",
		}

		// Attempt to create a new driver
		driver, err := NewPostgresDriver(opts)

		// Assert error and driver is nil
		assert.Error(t, err)
		assert.Nil(t, driver)

		// Error message should contain something about connection
		assert.Contains(t, err.Error(), "connect")
	})

	// Test case 3: Empty connection string
	t.Run("EmptyConnectionString", func(t *testing.T) {
		// Create client options with empty connection string
		opts := &types.ClientOpts{
			ConnectionString: "",
			Type:             "postgres",
		}

		// Attempt to create a new driver
		driver, err := NewPostgresDriver(opts)

		// Assert error and driver is nil
		assert.Error(t, err)
		assert.Nil(t, driver)
	})
}

func TestValidateDBAndTable(t *testing.T) {
	// Test case 1: Valid connection and table name
	t.Run("ValidConnectionAndTable", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// Create a mock object with a valid table name
		mockObj := &TestObject{TableNameValue: "valid_table"}

		// Call validateDBAndTable
		tableName, err := driver.validateDBAndTable(mockObj)

		// Assert no error and correct table name
		assert.NoError(t, err)
		assert.Equal(t, "valid_table", tableName)
	})

	// Test case 2: Valid connection but empty table name
	t.Run("EmptyTableName", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// Create a mock object with an empty table name
		mockObj := &nullableTableName{}

		// Call validateDBAndTable
		tableName, err := driver.validateDBAndTable(mockObj)

		// Assert error and empty table name
		assert.Error(t, err)
		assert.Equal(t, "", tableName)
		assert.Equal(t, types.ErrorEmptyTableName, err.Error())
	})

	// Test case 3: Nil database connection
	t.Run("NilDatabaseConnection", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, _ := setupTest(t)

		// Close the connection to simulate a nil db
		driver.Close()

		// Create a mock object with a valid table name
		mockObj := &TestObject{TableNameValue: "valid_table"}

		// Call validateDBAndTable
		tableName, err := driver.validateDBAndTable(mockObj)

		// Assert error and empty table name
		assert.Error(t, err)
		assert.Equal(t, "", tableName)
		assert.Equal(t, types.ErrorSessionClosed, err.Error())
	})

	// Test case 4: Nil object
	t.Run("NilObject", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// This should panic
		_, _ = driver.validateDBAndTable(nil)
	})
}
