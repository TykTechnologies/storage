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

	t.Run("map result carries only the listed columns", func(t *testing.T) {
		var got []map[string]interface{}
		err := driver.QueryFields(ctx, rows[0], &got, model.DBM{"_sort": "name", "_limit": 2}, []string{"id", "name", "value"})
		require.NoError(t, err)
		require.Len(t, got, 2, "_limit and _sort are honoured like in Query")
		assert.Equal(t, "Alice", got[0]["name"])
		assert.EqualValues(t, 45, got[0]["value"])
		assert.NotContains(t, got[0], "category", "columns outside the list are not read")
		assert.NotContains(t, got[0], "active")
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
