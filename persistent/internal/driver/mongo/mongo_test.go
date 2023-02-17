//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

func TestNewMongoDriver(t *testing.T) {
	t.Run("new driver with connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "mongodb://localhost:27017/test",
		})

		assert.Nil(t, err)
		assert.NotNil(t, newDriver)
		assert.NotNil(t, newDriver.lifeCycle)
		assert.NotNil(t, newDriver.options)
		assert.Nil(t, newDriver.client.Ping(context.Background(), nil))
	})
	t.Run("new driver with invalid connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "test",
		})

		assert.NotNil(t, err)
		assert.Equal(t, "error parsing uri: scheme must be \"mongodb\" or \"mongodb+srv\"", err.Error())
		assert.Nil(t, newDriver)
	})
	t.Run("new driver without connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{})

		assert.NotNil(t, err)
		assert.Equal(t, "can't connect without connection string", err.Error())
		assert.Nil(t, newDriver)
	})
}
