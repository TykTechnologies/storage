package mongo

import (
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestNewMongoDriver(t *testing.T){
	t.Run("new driver with connection string", func(t *testing.T) {
		newDriver ,err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "mongodb://localhost:27017/test",
		})

		assert.Nil(t, err)
		assert.NotNil(t, newDriver)
		assert.NotNil(t, newDriver.lifeCycle)
	})
	t.Run("new driver without connection string", func(t *testing.T) {
		newDriver ,err := NewMongoDriver(&model.ClientOpts{
		})

		assert.NotNil(t, err)
		assert.Equal(t, "can't connect without connection string",err.Error())
		assert.Nil(t, newDriver)
	})

}