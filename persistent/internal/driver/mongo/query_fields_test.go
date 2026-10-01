//go:build mongo7 || mongo6 || mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo7 mongo6 mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestQueryFields(t *testing.T) {
	defer cleanDB(t)
	driver, object := prepareEnvironment(t)
	ctx := context.Background()

	rows := []*dummyDBObject{
		{Name: "Bob", Email: "bob@example.com", Age: 25, Country: dummyCountryField{CountryName: "C1", Continent: "X"}},
		{Name: "Alice", Email: "alice@tyk.com", Age: 45, Country: dummyCountryField{CountryName: "C2", Continent: "Y"}},
		{Name: "Carl", Email: "carl@tyk.com", Age: 12, Country: dummyCountryField{CountryName: "C3", Continent: "Z"}},
	}
	for _, row := range rows {
		require.NoError(t, driver.Insert(ctx, row))
	}

	t.Run("map result carries only the listed fields and a model.ObjectID", func(t *testing.T) {
		var got []model.DBM
		err := driver.QueryFields(ctx, object, &got, model.DBM{"_sort": "name", "_limit": 2}, []string{"name", "age"})
		require.NoError(t, err)
		require.Len(t, got, 2, "_limit and _sort are honoured like in Query")
		assert.Equal(t, "Alice", got[0]["name"])
		assert.EqualValues(t, 45, got[0]["age"])
		assert.NotContains(t, got[0], "email", "fields outside the list are not read")
		assert.NotContains(t, got[0], "country")
		assert.Equal(t, rows[1].Id, got[0]["_id"], "_id is a model.ObjectID, as in Aggregate")
	})

	t.Run("typed result decodes the listed fields and leaves the rest zero", func(t *testing.T) {
		var got []dummyDBObject
		err := driver.QueryFields(ctx, object, &got, model.DBM{"email": model.DBM{"$text": "tyk.com"}, "_sort": "name"}, []string{"name"})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "Alice", got[0].Name)
		assert.Empty(t, got[0].Email)
		assert.Zero(t, got[0].Age)
	})

	t.Run("single map result", func(t *testing.T) {
		var got model.DBM
		err := driver.QueryFields(ctx, object, &got, model.DBM{"name": "Carl"}, []string{"age"})
		require.NoError(t, err)
		assert.EqualValues(t, 12, got["age"])
		assert.Equal(t, rows[2].Id, got["_id"])
		assert.NotContains(t, got, "name")
	})

	t.Run("no fields behaves like Query", func(t *testing.T) {
		var got []model.DBM
		require.NoError(t, driver.QueryFields(ctx, object, &got, model.DBM{"name": "Bob"}, nil))
		require.Len(t, got, 1)
		assert.Equal(t, "bob@example.com", got[0]["email"])
	})

	t.Run("cancelled context reaches the driver", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		var got []model.DBM
		assert.ErrorIs(t, driver.QueryFields(cancelled, object, &got, model.DBM{}, []string{"name"}), context.Canceled)
	})
}
