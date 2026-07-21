package testutil

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Suite configures a PersistentStorage conformance test run.
type Suite struct {
	// Storage is the driver under test.
	Storage types.PersistentStorage
	// NewObject returns a fresh, zeroed DBObject of the concrete type the driver expects.
	NewObject func() model.DBObject
	// IDFilter returns a DBM that selects a record by its ObjectID.
	// Mongo: model.DBM{"_id": id}
	// Postgres: model.DBM{"id": id}
	IDFilter func(model.ObjectID) model.DBM
}

// RunSuite verifies the behavioral contract of PersistentStorage across all drivers.
func RunSuite(t *testing.T, s Suite) {
	t.Helper()
	ctx := context.Background()

	// setup drops and recreates the table to guarantee a clean state.
	setup := func(t *testing.T) {
		t.Helper()

		if err := s.Storage.Drop(ctx, s.NewObject()); err != nil {
			t.Logf("Drop (pre-test cleanup): %v", err)
		}

		err := s.Storage.Migrate(ctx, []model.DBObject{s.NewObject()})
		require.NoError(t, err, "Migrate must succeed before each test")
	}

	t.Run("Ping", func(t *testing.T) {
		assert.NoError(t, s.Storage.Ping(ctx))
	})

	t.Run("HasTable", func(t *testing.T) {
		obj := s.NewObject()

		if err := s.Storage.Drop(ctx, obj); err != nil {
			t.Logf("Drop (pre-test cleanup): %v", err)
		}

		exists, err := s.Storage.HasTable(ctx, obj.TableName())
		require.NoError(t, err)
		assert.False(t, exists, "table should not exist after Drop")

		require.NoError(t, s.Storage.Migrate(ctx, []model.DBObject{obj}))

		exists, err = s.Storage.HasTable(ctx, obj.TableName())
		require.NoError(t, err)
		assert.True(t, exists, "table should exist after Migrate")
	})

	t.Run("InsertAndQueryByID", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))
		assert.NotEmpty(t, obj.GetObjectID(), "Insert must set ObjectID")

		result := s.NewObject()
		err := s.Storage.Query(ctx, result, result, s.IDFilter(obj.GetObjectID()))
		require.NoError(t, err)
		assert.Equal(t, obj.GetObjectID(), result.GetObjectID())
	})

	t.Run("InsertThenCount", func(t *testing.T) {
		setup(t)
		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 0, count, "count must be 0 before any inserts")

		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		count, err = s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "count must be 1 after one insert")
	})

	t.Run("InsertMultiple", func(t *testing.T) {
		setup(t)
		objs := []model.DBObject{s.NewObject(), s.NewObject(), s.NewObject()}
		require.NoError(t, s.Storage.Insert(ctx, objs...))

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, len(objs), count)
	})

	t.Run("DeleteByID", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		require.NoError(t, s.Storage.Delete(ctx, obj))

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 0, count, "count must be 0 after deleting the only record")
	})

	t.Run("DeleteNonExistentReturnsError", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		obj.SetObjectID(model.NewObjectID())

		err := s.Storage.Delete(ctx, obj)
		assert.Error(t, err, "deleting a non-existent record must return an error")
	})

	t.Run("UpdateExistingObject", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		err := s.Storage.Update(ctx, obj)
		assert.NoError(t, err)

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "update must not duplicate the record")
	})

	t.Run("UpdateNonExistentReturnsError", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		obj.SetObjectID(model.NewObjectID())

		err := s.Storage.Update(ctx, obj)
		assert.Error(t, err, "updating a non-existent record must return an error")
	})

	t.Run("BulkUpdateEmptyObjectsReturnsError", func(t *testing.T) {
		setup(t)
		err := s.Storage.BulkUpdate(ctx, []model.DBObject{})
		assert.Error(t, err, "BulkUpdate with empty slice must return an error")
	})

	t.Run("UpdateAllNoMatchReturnsError", func(t *testing.T) {
		setup(t)
		err := s.Storage.UpdateAll(ctx, s.NewObject(),
			model.DBM{"name": "nonexistent-xyzzy"},
			model.DBM{"$set": model.DBM{"name": "new-value"}})
		assert.Error(t, err, "UpdateAll with no matching rows must return an error")
	})

	t.Run("UpsertInsertsWhenNotFound", func(t *testing.T) {
		setup(t)
		freshID := model.NewObjectID()
		obj := s.NewObject()
		obj.SetObjectID(freshID)

		err := s.Storage.Upsert(ctx, obj, s.IDFilter(freshID), model.DBM{"$set": model.DBM{}})
		require.NoError(t, err)

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Upsert must insert when no record matches the query")
	})

	t.Run("UpsertUpdatesWhenFound", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))
		id := obj.GetObjectID()

		err := s.Storage.Upsert(ctx, obj, s.IDFilter(id), model.DBM{"$set": model.DBM{}})
		require.NoError(t, err)

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Upsert must not duplicate an existing record")
	})

	t.Run("UpsertNoDuplicatesUnderConcurrency", func(t *testing.T) {
		setup(t)
		freshID := model.NewObjectID()
		concurrency := 5
		errs := make(chan error, concurrency)

		for range concurrency {
			go func() {
				o := s.NewObject()
				o.SetObjectID(freshID)
				errs <- s.Storage.Upsert(ctx, o, s.IDFilter(freshID), model.DBM{"$set": model.DBM{}})
			}()
		}

		for range concurrency {
			assert.NoError(t, <-errs)
		}

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "concurrent Upserts on the same ID must not create duplicates")
	})

	t.Run("CreateAndGetIndexes", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		idx := model.Index{
			Name: "conformance_idx",
			Keys: []model.DBM{{"name": 1}},
		}

		err := s.Storage.CreateIndex(ctx, obj, idx)
		require.NoError(t, err)

		indexes, err := s.Storage.GetIndexes(ctx, obj)
		require.NoError(t, err)

		names := make([]string, 0, len(indexes))
		for _, ix := range indexes {
			names = append(names, ix.Name)
		}

		assert.Contains(t, names, "conformance_idx", "created index must be returned by GetIndexes")
	})

	t.Run("CleanIndexes", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		err := s.Storage.CreateIndex(ctx, obj, model.Index{
			Name: "conformance_clean_idx",
			Keys: []model.DBM{{"name": 1}},
		})
		require.NoError(t, err)

		require.NoError(t, s.Storage.CleanIndexes(ctx, obj))

		indexes, err := s.Storage.GetIndexes(ctx, obj)
		require.NoError(t, err)

		for _, ix := range indexes {
			assert.NotEqual(t, "conformance_clean_idx", ix.Name,
				"CleanIndexes must remove non-primary indexes")
		}
	})
}
