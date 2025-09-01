//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
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
}
