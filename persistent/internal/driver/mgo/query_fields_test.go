//go:build mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mgo

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
		{Name: "Bob", Email: "bob@example.com", Age: 25},
		{Name: "Alice", Email: "alice@tyk.com", Age: 45},
	}
	for _, row := range rows {
		require.NoError(t, driver.Insert(ctx, row))
	}

	var got []model.DBM
	err := driver.QueryFields(ctx, object, &got, model.DBM{"_sort": "name"}, []string{"name", "age"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "Alice", got[0]["name"])
	assert.Equal(t, 45, got[0]["age"])
	assert.NotContains(t, got[0], "email", "fields outside the list are not read")
	assert.Equal(t, rows[1].ID, got[0]["_id"], "_id is a model.ObjectID, as in Aggregate")

	var one model.DBM
	require.NoError(t, driver.QueryFields(ctx, object, &one, model.DBM{"name": "Bob"}, []string{"email"}))
	assert.Equal(t, "bob@example.com", one["email"])
	assert.NotContains(t, one, "age")
	assert.Equal(t, rows[0].ID, one["_id"])

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	assert.ErrorIs(t, driver.QueryFields(cancelled, object, &got, model.DBM{}, []string{"name"}), context.Canceled,
		"a cancelled context is refused before the query runs")
}
