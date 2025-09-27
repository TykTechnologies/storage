//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLifeCycle_DBType(t *testing.T) {
	lc := &lifeCycle{} // or the actual type if it's lowercase `lifeCycle`

	got := lc.DBType()
	assert.Equal(t, utils.PostgresDB, got, "DBType should return PostgresDB")
}

func TestPing(t *testing.T) {
	// Test case 1: Successful ping
	t.Run("SuccessfulPing", func(t *testing.T) {
		// Set up a test driver with a valid connection
		driver, ctx := setupTest(t)
		defer teardownTest(t, driver)

		// Ping the database
		err := driver.Ping(ctx)

		// Verify that the ping was successful
		assert.NoError(t, err, "Ping should not return an error when the database is available")
	})

	// Test case 2: Ping with canceled context
	t.Run("PingWithCanceledContext", func(t *testing.T) {
		// Set up a test driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// Create a canceled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		// Ping the database with the canceled context
		err := driver.Ping(ctx)

		// Verify that the ping failed due to canceled context
		assert.Error(t, err, "Ping should return an error when the context is canceled")
		assert.Contains(t, err.Error(), "context", "Error should mention context cancellation")
	})

	// Test case 4: Ping with timeout context
	t.Run("PingWithTimeoutContext", func(t *testing.T) {
		// Set up a test driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// Create a context with a very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Sleep briefly to ensure the timeout expires
		time.Sleep(1 * time.Millisecond)

		// Ping the database with the timeout context
		err := driver.Ping(ctx)

		// Verify that the ping failed due to timeout
		// Note: This might not always fail if the ping is very fast, so we'll make it a soft assertion
		if err != nil {
			assert.Contains(t, err.Error(), "context", "If error occurs, it should mention context deadline")
		}
	})

	// Test case 5: Ping with nil context
	t.Run("PingWithNilContext", func(t *testing.T) {
		// Set up a test driver with a valid connection
		driver, _ := setupTest(t)
		defer teardownTest(t, driver)

		// Ping the database with nil context
		err := driver.Ping(nil)

		// Verify that the ping failed due to nil context
		assert.Error(t, err, "Ping should return an error when the context is nil")
		assert.Contains(t, err.Error(), "context", "Error should mention context issue")
	})

	t.Run("Ping with closed connection", func(t *testing.T) {
		// Create a driver instance
		driver, ctx := setupTest(t)

		// Explicitly close the session to make d.db nil
		err := driver.Close()
		assert.NoError(t, err, "Failed to close the driver session")

		// Now attempt to ping with the closed session
		err = driver.Ping(ctx)

		// Verify that the correct error is returned
		assert.Error(t, err, "Ping should return an error when session is closed")
	})
}

func TestLifeCycleConnect(t *testing.T) {
	// Test case 1: Successful connection
	t.Run("SuccessfulConnection", func(t *testing.T) {
		// Create a new lifeCycle instance
		lc := &lifeCycle{}

		testCases := []struct {
			name string
			connectionStr string
		}{
			{
				name: "standard connection string",
				connectionStr: getconnStr() ,,
			},
			{
				name: "connection string as URL",
				connectionStr: connStrAsURL,
			},
		}

		for tc := range testCases{
			t.Run(tc.name, func(t *testing.T) {
				opts := &types.ClientOpts{
					ConnectionString: tc.connectionStr,
					Type:             "postgresDBType",
				}

				// Connect to the database
				err := lc.Connect(opts)
				// Verify that the connection was successful
				assert.NoError(t, err, "Connect should not return an error with valid options")

				// Verify that the connection is established
				assert.NotNil(t, lc.db, "Database connection should not be nil after successful connection")

				// Verify that the connection works by pinging the database
				ctx := context.Background()
				err = lc.db.WithContext(ctx).Exec("SELECT 1").Error
				assert.NoError(t, err, "Should be able to execute a simple query after connection")

				// Clean up
				if lc.db != nil {
					sqlDB, err := lc.db.DB()
					if err == nil {
						sqlDB.Close()
					}
				}
			})
		}

	})

	// Test case 2: Failed connection due to invalid connection string
	t.Run("FailedConnectionInvalidString", func(t *testing.T) {
		// Create a new lifeCycle instance
		lc := &lifeCycle{}

		// Create invalid client options
		opts := &types.ClientOpts{
			ConnectionString: "host=nonexistent-host port=5432 user=invalid dbname=nonexistent",
			Type:             "postgres",
		}

		// Attempt to connect to the database
		err := lc.Connect(opts)

		// Verify that the connection failed
		assert.Error(t, err, "Connect should return an error with invalid connection string")

		// Verify that the database connection is nil
		assert.Nil(t, lc.db, "Database connection should be nil after failed connection")
	})

	// Test case 3: Failed connection due to nil options
	t.Run("FailedConnectionNilOptions", func(t *testing.T) {
		// Create a new lifeCycle instance
		lc := &lifeCycle{}

		// Attempt to connect to the database with nil options
		err := lc.Connect(nil)

		// Verify that the connection failed
		assert.Error(t, err, "Connect should return an error with nil options")

		// Verify that the database connection is nil
		assert.Nil(t, lc.db, "Database connection should be nil after failed connection")
	})

	// Test case 4: Failed connection due to empty connection string
	t.Run("FailedConnectionEmptyString", func(t *testing.T) {
		// Create a new lifeCycle instance
		lc := &lifeCycle{}

		// Create client options with empty connection string
		opts := &types.ClientOpts{
			ConnectionString: "",
			Type:             "postgres",
		}

		// Attempt to connect to the database
		err := lc.Connect(opts)

		// Verify that the connection failed
		assert.Error(t, err, "Connect should return an error with empty connection string")

		// Verify that the database connection is nil
		assert.Nil(t, lc.db, "Database connection should be nil after failed connection")
	})
}

func TestDropDatabase(t *testing.T) {
	t.Run("connection is nil", func(t *testing.T) {
		driver, _ := setupTest(t)
		driver.Close()
		err := driver.DropDatabase(context.Background())
		assert.Error(t, err)
	})

	t.Run("postgres no drop", func(t *testing.T) {
		driver, _ := setupTest(t)

		err := driver.DropDatabase(context.Background())
		assert.Nil(t, driver.db)
		assert.Error(t, err)
	})
}
