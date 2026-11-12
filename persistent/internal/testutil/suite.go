// Package testutil provides a driver-agnostic conformance test suite for the
// persistent storage layer. Each driver (mgo, official Mongo, Postgres) passes
// its storage to RunSuite, which runs the same model, the same filters and the
// same assertions against every driver so behavioral drift between them is
// caught in CI.
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
)

// Entity is the one model every driver stores, tagged for Mongo and for SQL,
// so the suite proves a single model definition works on all of them.
type Entity struct {
	ID        model.ObjectID `bson:"_id,omitempty" json:"id" gorm:"primaryKey;column:id"`
	Name      string         `bson:"name" json:"name"`
	Category  string         `bson:"category" json:"category"`
	Value     int            `bson:"value" json:"value"`
	ExpiresAt time.Time      `bson:"expires_at" json:"expires_at"`
}

// TableName returns the collection or table the suite works on.
func (e *Entity) TableName() string { return "conformance_entities" }

// GetObjectID returns the record ID.
func (e *Entity) GetObjectID() model.ObjectID { return e.ID }

// SetObjectID sets the record ID.
func (e *Entity) SetObjectID(id model.ObjectID) { e.ID = id }

// Suite configures a PersistentStorage conformance test run.
type Suite struct {
	// Storage is the driver under test.
	Storage types.PersistentStorage
}

func newEntity() model.DBObject { return &Entity{} }

// idFilter selects a record by ID the way Mongo callers do; every driver must accept it.
func idFilter(id model.ObjectID) model.DBM { return model.DBM{"_id": id} }

// RunSuite verifies the behavioral contract of PersistentStorage across all drivers.
func RunSuite(t *testing.T, s Suite) {
	t.Helper()

	ctx := context.Background()

	// setup drops and recreates the table to guarantee a clean state.
	setup := func(t *testing.T) {
		t.Helper()

		if err := s.Storage.Drop(ctx, newEntity()); err != nil {
			t.Logf("Drop (pre-test cleanup): %v", err)
		}

		err := s.Storage.Migrate(ctx, []model.DBObject{newEntity()})
		require.NoError(t, err, "Migrate must succeed before each test")
	}

	t.Run("Ping", func(t *testing.T) {
		assert.NoError(t, s.Storage.Ping(ctx))
	})

	t.Run("HasTable", func(t *testing.T) {
		obj := newEntity()

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

		obj := newEntity()
		require.NoError(t, s.Storage.Insert(ctx, obj))
		assert.NotEmpty(t, obj.GetObjectID(), "Insert must set ObjectID")

		result := newEntity()
		err := s.Storage.Query(ctx, result, result, idFilter(obj.GetObjectID()))
		require.NoError(t, err)
		assert.Equal(t, obj.GetObjectID(), result.GetObjectID())
	})

	t.Run("InsertThenCount", func(t *testing.T) {
		setup(t)

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 0, count, "count must be 0 before any inserts")

		obj := newEntity()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		count, err = s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "count must be 1 after one insert")
	})

	t.Run("InsertMultiple", func(t *testing.T) {
		setup(t)

		objs := []model.DBObject{newEntity(), newEntity(), newEntity()}
		require.NoError(t, s.Storage.Insert(ctx, objs...))

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, len(objs), count)
	})

	t.Run("DeleteByID", func(t *testing.T) {
		setup(t)

		obj := newEntity()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		require.NoError(t, s.Storage.Delete(ctx, obj))

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 0, count, "count must be 0 after deleting the only record")
	})

	t.Run("DeleteNonExistentReturnsError", func(t *testing.T) {
		setup(t)

		obj := newEntity()
		obj.SetObjectID(model.NewObjectID())

		err := s.Storage.Delete(ctx, obj)
		assert.Error(t, err, "deleting a non-existent record must return an error")
	})

	t.Run("UpdateExistingObject", func(t *testing.T) {
		setup(t)

		obj := newEntity()
		require.NoError(t, s.Storage.Insert(ctx, obj))

		err := s.Storage.Update(ctx, obj)
		assert.NoError(t, err)

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "update must not duplicate the record")
	})

	t.Run("UpdateNonExistentReturnsError", func(t *testing.T) {
		setup(t)

		obj := newEntity()
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

		err := s.Storage.UpdateAll(ctx, newEntity(),
			model.DBM{"name": "nonexistent-xyzzy"},
			model.DBM{"$set": model.DBM{"name": "new-value"}})
		assert.Error(t, err, "UpdateAll with no matching rows must return an error")
	})

	t.Run("UpsertInsertsWhenNotFound", func(t *testing.T) {
		setup(t)

		freshID := model.NewObjectID()
		obj := newEntity()
		obj.SetObjectID(freshID)

		err := s.Storage.Upsert(ctx, obj, idFilter(freshID), model.DBM{"$set": model.DBM{}})
		require.NoError(t, err)

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Upsert must insert when no record matches the query")
	})

	t.Run("UpsertUpdatesWhenFound", func(t *testing.T) {
		setup(t)

		obj := newEntity()
		require.NoError(t, s.Storage.Insert(ctx, obj))
		id := obj.GetObjectID()

		err := s.Storage.Upsert(ctx, obj, idFilter(id), model.DBM{"$set": model.DBM{}})
		require.NoError(t, err)

		count, err := s.Storage.Count(ctx, newEntity())
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
				o := newEntity()
				o.SetObjectID(freshID)
				errs <- s.Storage.Upsert(ctx, o, idFilter(freshID), model.DBM{"$set": model.DBM{}})
			}()
		}

		for range concurrency {
			assert.NoError(t, <-errs)
		}

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 1, count, "concurrent Upserts on the same ID must not create duplicates")
	})

	t.Run("CreateAndGetIndexes", func(t *testing.T) {
		setup(t)

		obj := newEntity()
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

		obj := newEntity()
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

	// seed stores three known rows and returns them.
	seed := func(t *testing.T) []*Entity {
		t.Helper()
		setup(t)

		rows := []*Entity{
			{Name: "one", Category: "a", Value: 10},
			{Name: "two", Category: "b", Value: 20},
			{Name: "three", Category: "a", Value: 30},
		}
		for _, r := range rows {
			require.NoError(t, s.Storage.Insert(ctx, r))
		}

		return rows
	}

	fetch := func(t *testing.T, id model.ObjectID) *Entity {
		t.Helper()

		got := &Entity{}
		require.NoError(t, s.Storage.Query(ctx, &Entity{}, got, idFilter(id)))

		return got
	}

	names := func(t *testing.T, filter model.DBM) []string {
		t.Helper()

		filter["_sort"] = "value"

		var found []Entity
		require.NoError(t, s.Storage.Query(ctx, &Entity{}, &found, filter))

		out := make([]string, 0, len(found))
		for i := range found {
			out = append(out, found[i].Name)
		}

		return out
	}

	t.Run("UpdateWritesFields", func(t *testing.T) {
		rows := seed(t)

		rows[0].Name = "renamed"
		rows[0].Value = 11
		require.NoError(t, s.Storage.Update(ctx, rows[0]))

		got := fetch(t, rows[0].ID)
		assert.Equal(t, "renamed", got.Name)
		assert.Equal(t, 11, got.Value)
		assert.Equal(t, "two", fetch(t, rows[1].ID).Name, "other rows must be untouched")
	})

	t.Run("UpsertInsertWritesFields", func(t *testing.T) {
		setup(t)

		id := model.NewObjectID()
		err := s.Storage.Upsert(ctx, &Entity{}, idFilter(id),
			model.DBM{"$set": model.DBM{"name": "created", "value": 7}})
		require.NoError(t, err)

		got := fetch(t, id)
		assert.Equal(t, "created", got.Name)
		assert.Equal(t, 7, got.Value)
	})

	t.Run("UpsertUpdateWritesFields", func(t *testing.T) {
		rows := seed(t)

		result := &Entity{}
		err := s.Storage.Upsert(ctx, result, idFilter(rows[1].ID),
			model.DBM{"$set": model.DBM{"name": "changed"}})
		require.NoError(t, err)
		assert.Equal(t, "changed", result.Name, "Upsert must return the stored record")

		got := fetch(t, rows[1].ID)
		assert.Equal(t, "changed", got.Name)
		assert.Equal(t, 20, got.Value, "fields outside $set must be kept")

		count, err := s.Storage.Count(ctx, newEntity())
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("OrBranchFieldsAreANDed", func(t *testing.T) {
		seed(t)

		got := names(t, model.DBM{"$or": []model.DBM{
			{"category": "a", "value": 10},
			{"value": 20},
		}})
		assert.Equal(t, []string{"one", "two"}, got)
	})

	t.Run("OrKeepsSiblingFilter", func(t *testing.T) {
		seed(t)

		got := names(t, model.DBM{
			"category": "a",
			"$or":      []model.DBM{{"value": 10}, {"value": 20}},
		})
		assert.Equal(t, []string{"one"}, got)
	})

	t.Run("OrAsGenericList", func(t *testing.T) {
		seed(t)

		got := names(t, model.DBM{
			"category": "a",
			"$or":      []interface{}{model.DBM{"name": "two"}, model.DBM{"name": "three"}},
		})
		assert.Equal(t, []string{"three"}, got)
	})

	t.Run("OrByID", func(t *testing.T) {
		rows := seed(t)

		got := names(t, model.DBM{"$or": []model.DBM{idFilter(rows[0].ID), idFilter(rows[2].ID)}})
		assert.Equal(t, []string{"one", "three"}, got)
	})

	t.Run("TTLIndexIsReported", func(t *testing.T) {
		setup(t)

		err := s.Storage.CreateIndex(ctx, newEntity(), model.Index{
			Name:       "conformance_ttl_idx",
			Keys:       []model.DBM{{"expires_at": 1}},
			IsTTLIndex: true,
			TTL:        3600,
		})
		require.NoError(t, err)

		indexes, err := s.Storage.GetIndexes(ctx, newEntity())
		require.NoError(t, err)

		var found bool

		for _, ix := range indexes {
			if ix.Name == "conformance_ttl_idx" {
				found = true

				assert.True(t, ix.IsTTLIndex, "TTL index must be reported as TTL")
				assert.Equal(t, 3600, ix.TTL)
			}
		}

		assert.True(t, found, "TTL index must be returned by GetIndexes")
	})
}
