package postgres

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

type dummyDBObject struct {
	Id                model.ObjectID    `json:"_id,omitempty"`
	Name              string            `json:"name"`
	Email             string            `json:"email"`
	Country           dummyCountryField `json:"country"`
	Age               int               `json:"age"`
	invalidCollection bool
}

type dummyCountryField struct {
	CountryName string `json:"country_name"`
	Continent   string `json:"continent"`
}

func (d *dummyDBObject) GetObjectID() model.ObjectID {
	return d.Id
}

func (d *dummyDBObject) SetObjectID(id model.ObjectID) {
	d.Id = id
}

func (d *dummyDBObject) TableName() string {
	if d.invalidCollection {
		return ""
	}
	return "dummy"
}

func prepareEnvironment(t *testing.T) (*driver, *dummyDBObject) {
	t.Helper()
	postgres, err := NewPostgresDriver(&types.ClientOpts{
		UseSSL:           false,
		ConnectionString: "host=localhost port=5432 user=postgres dbname=test password=secr3t",
	})
	if err != nil {
		t.Fatal(err)
	}
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}
	return postgres, object
}

func cleanDB(t *testing.T) {
	t.Helper()
	d, _ := prepareEnvironment(t)
	helper.ErrPrint(d.DropDatabase(context.Background()))
}

func TestNewPostgresDriver(t *testing.T) {
	defer cleanDB(t)

	t.Run("new driver with connection string", func(t *testing.T) {
		newDriver, err := NewPostgresDriver(&types.ClientOpts{
			UseSSL:           false,
			ConnectionString: "host=localhost port=5432 user=postgres dbname=test password=secr3t",
		})

		assert.NoError(t, err)
		assert.NotNil(t, newDriver)
		assert.NotNil(t, newDriver.db)
		assert.Nil(t, newDriver.Ping(context.Background()))
	})

	t.Run("new driver with invalid connection string", func(t *testing.T) {
		newDriver, err := NewPostgresDriver(&types.ClientOpts{
			ConnectionString:  "invalid",
			ConnectionTimeout: 1,
		})
		assert.NotNil(t, err)
		assert.Nil(t, newDriver)
	})

	t.Run("new driver without connection string", func(t *testing.T) {
		newDriver, err := NewPostgresDriver(&types.ClientOpts{})
		assert.NotNil(t, err)
		assert.Nil(t, newDriver)
	})
}
