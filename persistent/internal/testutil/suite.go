package testutil

import (
	"context"
	"sync"
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
		_ = s.Storage.Drop(ctx, s.NewObject())
		err := s.Storage.Migrate(ctx, []model.DBObject{s.NewObject()})
		require.NoError(t, err, "Migrate must succeed before each test")
	}

	t.Run("Ping", func(t *testing.T) {
		assert.NoError(t, s.Storage.Ping(ctx))
	})

	t.Run("HasTable", func(t *testing.T) {
		obj := s.NewObject()
		_ = s.Storage.Drop(ctx, obj)

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
		n := 3
		objs := make([]model.DBObject, n)
		for i := range objs {
			objs[i] = s.NewObject()
		}
		require.NoError(t, s.Storage.Insert(ctx, objs...))

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, n, count)
	})

	t.Run("DeleteByID", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		// Delete using the object's own ID (no explicit filter)
		require.NoError(t, s.Storage.Delete(ctx, obj))

		count, err := s.Storage.Count(ctx, s.NewObject())
		require.NoError(t, err)
		assert.Equal(t, 0, count, "count must be 0 after deleting the only record")
	})

	t.Run("DeleteNonExistentReturnsError", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		obj.SetObjectID(model.NewObjectID()) // random ID, nothing in the table

		err := s.Storage.Delete(ctx, obj)
		assert.Error(t, err, "deleting a non-existent record must return an error")
	})

	t.Run("UpdateExistingObject", func(t *testing.T) {
		setup(t)
		obj := s.NewObject()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		// Update with the same values — must not return an error.
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
		// No records inserted; any filter produces 0 affected rows.
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

		// Upsert the same record again — must update, not insert.
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

		errs := make([]error, concurrency)
		var wg sync.WaitGroup

		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				o := s.NewObject()
				o.SetObjectID(freshID)
				errs[idx] = s.Storage.Upsert(ctx, o, s.IDFilter(freshID), model.DBM{"$set": model.DBM{}})
			}(i)
		}

		wg.Wait()

		for _, err := range errs {
			assert.NoError(t, err)
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

		found := false
		for _, ix := range indexes {
			if ix.Name == "conformance_idx" {
				found = true
				break
			}
		}
		assert.True(t, found, "created index must be returned by GetIndexes")
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
