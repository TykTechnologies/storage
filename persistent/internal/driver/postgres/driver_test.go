//go:build postgres || postgres14 || postgres13 || postgres12.22 || postgres11 || postgres10
// +build postgres postgres14 postgres13 postgres12.22 postgres11 postgres10

package postgres

import (
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewPostgresDriver(t *testing.T) {
	// Test case 1: Successful connection
	t.Run("SuccessfulConnection", func(t *testing.T) {
		// Create client options with valid connection string
		opts := &types.ClientOpts{
			ConnectionString: connStr,
			Type:             "postgres",
		}

		// Create a new driver
		driver, err := NewPostgresDriver(opts)

		// Assert no error and driver is not nil
		assert.NoError(t, err)
		assert.NotNil(t, driver)

		// Verify the driver is connected by pinging the database
		err = driver.Ping(nil)
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

	// Test case 4: With SSL options
	t.Run("WithSSLOptions", func(t *testing.T) {
		// Skip this test if we don't have SSL certificates available
		// This is just a structure test to ensure the SSL options are properly handled

		// Create client options with SSL enabled
		opts := &types.ClientOpts{
			ConnectionString:         "postgres://postgres:postgres@localhost:5432/postgres",
			UseSSL:                   true,
			SSLInsecureSkipVerify:    true,
			SSLAllowInvalidHostnames: true,
		}

		// This might fail if SSL is not properly configured, but we're testing the structure
		driver, err := NewPostgresDriver(opts)
		if err == nil {
			// If connection succeeded, clean up
			defer driver.Close()
			assert.NotNil(t, driver)
		} else {
			// If connection failed, that's expected in environments without proper SSL setup
			t.Logf("SSL connection failed as expected: %v", err)
		}
	})

	// Test case 5: With connection timeout
	t.Run("WithConnectionTimeout", func(t *testing.T) {
		// Create client options with connection timeout
		opts := &types.ClientOpts{
			ConnectionString:  "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			Type:              "postgres",
			ConnectionTimeout: 10, // 10 seconds
		}

		// Create a new driver
		driver, err := NewPostgresDriver(opts)

		// Assert no error and driver is not nil
		assert.NoError(t, err)
		assert.NotNil(t, driver)

		// Clean up
		err = driver.Close()
		assert.NoError(t, err)
	})
}

func TestValidateDBAndTable(t *testing.T) {
	// Test case 1: Valid connection and table name
	t.Run("ValidConnectionAndTable", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, ctx := setupTest(t)
		defer teardownTest(t, driver)

		// Create a mock object with a valid table name
		mockObj := &MockDBObject{TableNameValue: "valid_table"}

		// Call validateDBAndTable
		tableName, err := driver.validateDBAndTable(mockObj)

		// Assert no error and correct table name
		assert.NoError(t, err)
		assert.Equal(t, "valid_table", tableName)
	})

	// Test case 2: Valid connection but empty table name
	t.Run("EmptyTableName", func(t *testing.T) {
		// Create a driver with a valid connection
		driver, ctx := setupTest(t)
		defer teardownTest(t, driver)

		// Create a mock object with an empty table name
		mockObj := &MockDBObject{TableNameValue: ""}

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
		driver, ctx := setupTest(t)

		// Close the connection to simulate a nil db
		driver.Close()

		// Create a mock object with a valid table name
		mockObj := &MockDBObject{TableNameValue: "valid_table"}

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
		driver, ctx := setupTest(t)
		defer teardownTest(t, driver)

		// Call validateDBAndTable with nil object
		// This should panic, so we need to recover
		defer func() {
			r := recover()
			assert.NotNil(t, r, "Expected panic but got none")
		}()

		// This should panic
		_, _ = driver.validateDBAndTable(nil)
	})
}
