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

	t.Run("map rows always carry _id even when it was not listed", func(t *testing.T) {
		var got []model.DBM
		require.NoError(t, driver.QueryFields(ctx, object, &got, model.DBM{"name": "Bob"}, []string{"name"}))
		require.Len(t, got, 1)
		assert.Equal(t, rows[0].Id, got[0]["_id"], "_id is a non-empty model.ObjectID like on Postgres")
		assert.NotContains(t, got[0], "email")
	})

	t.Run("id is an ordinary field, independent from _id", func(t *testing.T) {
		doc := &dummyWithPublicID{PublicID: "pol-42", Name: "Policy"}
		require.NoError(t, driver.Insert(ctx, doc))

		var got []model.DBM
		require.NoError(t, driver.QueryFields(ctx, doc, &got, model.DBM{"id": "pol-42"}, []string{"id"}))
		require.Len(t, got, 1)
		assert.Equal(t, doc.ID, got[0]["_id"], "_id is the normalised object id")
		assert.Equal(t, "pol-42", got[0]["id"], "id is the document's own field, untouched")
		assert.NotContains(t, got[0], "name")

		var both []model.DBM
		require.NoError(t, driver.QueryFields(ctx, doc, &both, model.DBM{"id": "pol-42"}, []string{"_id", "id"}))
		require.Len(t, both, 1)
		assert.Equal(t, doc.ID, both[0]["_id"])
		assert.Equal(t, "pol-42", both[0]["id"])
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

// dummyWithPublicID has an id field of its own next to the object id, like several Dashboard models.
type dummyWithPublicID struct {
	ID       model.ObjectID `bson:"_id,omitempty"`
	PublicID string         `bson:"id"`
	Name     string         `bson:"name"`
}

func (d *dummyWithPublicID) GetObjectID() model.ObjectID   { return d.ID }
func (d *dummyWithPublicID) SetObjectID(id model.ObjectID) { d.ID = id }
func (d *dummyWithPublicID) TableName() string             { return "dummy" }
