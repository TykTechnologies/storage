//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestQueryFields(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	rows := []*TestObject{
		{Name: "Bob", Value: 25, Active: true, Category: "a", CreatedAt: time.Now()},
		{Name: "Alice", Value: 45, Active: false, Category: "b", CreatedAt: time.Now()},
		{Name: "Carl", Value: 12, Active: true, Category: "a", CreatedAt: time.Now()},
	}
	for _, row := range rows {
		require.NoError(t, driver.Insert(ctx, row))
	}

	t.Run("map result carries only the listed columns and _id as a model.ObjectID", func(t *testing.T) {
		var got []model.DBM
		err := driver.QueryFields(ctx, rows[0], &got, model.DBM{"_sort": "name", "_limit": 2}, []string{"_id", "name", "value", "name", " "})
		require.NoError(t, err)
		require.Len(t, got, 2, "_limit and _sort are honoured like in Query")
		assert.Equal(t, "Alice", got[0]["name"])
		assert.EqualValues(t, 45, got[0]["value"])
		assert.Equal(t, rows[1].ID, got[0]["_id"], "_id names the id column and comes back as a model.ObjectID, like on Mongo")
		assert.NotContains(t, got[0], "id", "only _id was asked for")
		assert.NotContains(t, got[0], "category", "columns outside the list are not read")
		assert.NotContains(t, got[0], "active")
	})

	t.Run("single map result", func(t *testing.T) {
		var got model.DBM
		require.NoError(t, driver.QueryFields(ctx, rows[0], &got, model.DBM{"name": "Carl"}, []string{"value"}))
		assert.EqualValues(t, 12, got["value"])
		assert.NotContains(t, got, "name")
		var missing model.DBM
		assert.ErrorIs(t, driver.QueryFields(ctx, rows[0], &missing, model.DBM{"name": "nobody"}, []string{"value"}), sql.ErrNoRows)
	})

	t.Run("a field name that is not an identifier is refused", func(t *testing.T) {
		var got []model.DBM
		err := driver.QueryFields(ctx, rows[0], &got, model.DBM{}, []string{"name, (SELECT 1) AS x"})
		assert.ErrorContains(t, err, "invalid field name")
	})

	t.Run("map rows always carry _id even when it was not listed", func(t *testing.T) {
		var got []model.DBM
		require.NoError(t, driver.QueryFields(ctx, rows[0], &got, model.DBM{"name": "Bob"}, []string{"name"}))
		require.Len(t, got, 1)
		assert.Equal(t, rows[0].ID, got[0]["_id"], "_id is a non-empty model.ObjectID like on Mongo")
		assert.Equal(t, "Bob", got[0]["name"])
		assert.NotContains(t, got[0], "value")
	})

	t.Run("listing both _id and id returns both", func(t *testing.T) {
		var got []model.DBM
		require.NoError(t, driver.QueryFields(ctx, rows[0], &got, model.DBM{"name": "Bob"}, []string{"_id", "id"}))
		require.Len(t, got, 1)
		assert.Equal(t, rows[0].ID, got[0]["_id"])
		assert.Equal(t, rows[0].ID.Hex(), got[0]["id"], "the explicitly requested id column is kept as stored")
	})

	t.Run("typed result leaves unselected columns zero", func(t *testing.T) {
		var got []TestObject
		err := driver.QueryFields(ctx, rows[0], &got, model.DBM{"category": "a", "_sort": "name"}, []string{"name"})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "Bob", got[0].Name)
		assert.Zero(t, got[0].Value)
		assert.Empty(t, got[0].Category)
	})

	t.Run("no fields behaves like Query", func(t *testing.T) {
		var got []TestObject
		require.NoError(t, driver.QueryFields(ctx, rows[0], &got, model.DBM{"name": "Carl"}, nil))
		require.Len(t, got, 1)
		assert.Equal(t, 12, got[0].Value)
	})

	t.Run("no rows reports sql.ErrNoRows like Query", func(t *testing.T) {
		var got []TestObject
		assert.ErrorIs(t, driver.QueryFields(ctx, rows[0], &got, model.DBM{"name": "nobody"}, []string{"name"}), sql.ErrNoRows)
	})

	t.Run("cancelled context reaches the driver", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		var got []TestObject
		assert.ErrorIs(t, driver.QueryFields(cancelled, rows[0], &got, model.DBM{}, []string{"name"}), context.Canceled)
	})
}
