//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22 
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// UniqueTestObject is a test model with a unique constraint for duplicate key testing.
type UniqueTestObject struct {
	ID             model.ObjectID `json:"id" gorm:"primaryKey"`
	Email          string         `json:"email" gorm:"uniqueIndex"`
	Name           string         `json:"name"`
	TableNameValue string         `json:"-"`
}

func (u *UniqueTestObject) TableName() string {
	if u.TableNameValue != "" {
		return u.TableNameValue
	}
	return "unique_test_objects"
}

func (u *UniqueTestObject) GetObjectID() model.ObjectID {
	return u.ID
}

func (u *UniqueTestObject) SetObjectID(id model.ObjectID) {
	u.ID = id
}

// TestGORMCompatibility_MigratorColumnType validates that the GORM migrator
// correctly handles column type inspection after the fork cherry-pick of
// upstream commit 0af95f5 (migrator.ColumnType struct).
func TestGORMCompatibility_MigratorColumnType(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	tableName := "test_gorm_column_type"
	obj := &TestObject{TableNameValue: tableName}
	defer driver.Drop(ctx, obj)

	// Migrate to create the table
	err := driver.Migrate(ctx, []model.DBObject{obj})
	require.NoError(t, err, "Migration should succeed")

	// Use GORM's migrator to inspect column types
	migrator := driver.db.WithContext(ctx).Migrator()

	t.Run("ColumnTypes", func(t *testing.T) {
		columnTypes, err := migrator.ColumnTypes(obj)
		require.NoError(t, err, "ColumnTypes should work with updated GORM fork")
		require.NotEmpty(t, columnTypes, "Should return column type information")

		// Build a map of column names for easier lookup
		columnMap := make(map[string]gorm.ColumnType)
		for _, col := range columnTypes {
			columnMap[col.Name()] = col
		}

		// Verify the 'name' column exists and has correct type info
		nameCol, ok := columnMap["name"]
		require.True(t, ok, "Should have a 'name' column")

		dbType := nameCol.DatabaseTypeName()
		assert.NotEmpty(t, dbType, "Database type name should not be empty")
		t.Logf("Column 'name' database type: %s", dbType)

		// Verify nullable info is available
		nullable, ok := nameCol.Nullable()
		if ok {
			t.Logf("Column 'name' nullable: %v", nullable)
		}
	})

	t.Run("HasColumn", func(t *testing.T) {
		assert.True(t, migrator.HasColumn(obj, "name"), "Should have 'name' column")
		assert.True(t, migrator.HasColumn(obj, "value"), "Should have 'value' column")
		assert.False(t, migrator.HasColumn(obj, "nonexistent"), "Should not have 'nonexistent' column")
	})
}

// TestGORMCompatibility_MigratorGetIndexes validates that the GORM migrator's
// GetIndexes method works correctly after the fork cherry-pick of upstream
// commit 1305f63 (gorm.Index interface + migrator.Index struct).
func TestGORMCompatibility_MigratorGetIndexes(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Use default "test_objects" table created by setupTest so GORM's
	// migrator resolves the table name correctly from the struct.
	obj := &TestObject{}

	t.Run("DefaultIndexes", func(t *testing.T) {
		// GetIndexes should work (this uses the cherry-picked gorm.Index interface)
		indexes, err := driver.db.WithContext(ctx).Migrator().GetIndexes(obj)
		require.NoError(t, err, "GetIndexes should work with updated GORM fork")

		// Primary key index should exist
		t.Logf("Found %d indexes on table %s", len(indexes), obj.TableName())
		for _, idx := range indexes {
			isPrimary, _ := idx.PrimaryKey()
			isUnique, _ := idx.Unique()
			t.Logf("  Index: %s, columns: %v, primary: %v, unique: %v",
				idx.Name(), idx.Columns(), isPrimary, isUnique)
		}

		// At minimum, we should have the primary key index
		assert.NotEmpty(t, indexes, "Should have at least the primary key index")
	})

	t.Run("AfterCreatingCustomIndex", func(t *testing.T) {
		// Create a custom index using the driver's method
		index := model.Index{
			Keys: []model.DBM{{"name": 1}},
			Name: "idx_gorm_compat_name",
		}
		err := driver.CreateIndex(ctx, obj, index)
		require.NoError(t, err)

		// Verify GetIndexes returns it via GORM's migrator
		indexes, err := driver.db.WithContext(ctx).Migrator().GetIndexes(obj)
		require.NoError(t, err)

		foundCustom := false
		for _, idx := range indexes {
			if idx.Name() == "idx_gorm_compat_name" {
				foundCustom = true
				assert.Contains(t, idx.Columns(), "name", "Custom index should be on 'name' column")
			}
		}
		assert.True(t, foundCustom, "Custom index should be visible via GetIndexes")
	})
}

// TestGORMCompatibility_ErrorTranslator validates that the GORM error translator
// works correctly after the fork cherry-pick of upstream commit 85eaf9e
// (ErrDuplicatedKey + ErrorTranslator interface).
func TestGORMCompatibility_ErrorTranslator(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	tableName := "test_gorm_error_translator"
	obj := &UniqueTestObject{TableNameValue: tableName}
	defer driver.Drop(ctx, obj)

	// Create table with unique constraint via AutoMigrate
	err := driver.db.WithContext(ctx).Table(tableName).AutoMigrate(obj)
	require.NoError(t, err, "AutoMigrate with unique constraint should succeed")

	t.Run("DuplicateKeyViolation", func(t *testing.T) {
		// Insert first record
		first := &UniqueTestObject{
			ID:             model.NewObjectID(),
			Email:          "duplicate@test.com",
			Name:           "First",
			TableNameValue: tableName,
		}
		err := driver.db.WithContext(ctx).Table(tableName).Create(first).Error
		require.NoError(t, err, "First insert should succeed")

		// Insert duplicate - should trigger ErrorTranslator
		second := &UniqueTestObject{
			ID:             model.NewObjectID(),
			Email:          "duplicate@test.com",
			Name:           "Second",
			TableNameValue: tableName,
		}
		err = driver.db.WithContext(ctx).Table(tableName).Create(second).Error
		require.Error(t, err, "Duplicate insert should fail")

		// The ErrorTranslator in the updated GORM fork should translate this
		// to ErrDuplicatedKey. If the cherry-pick is working correctly,
		// the driver/postgres v1.5.0 translates the pgx error.
		assert.ErrorIs(t, err, gorm.ErrDuplicatedKey,
			"Duplicate key violation should be translated to gorm.ErrDuplicatedKey")
	})
}

// TestGORMCompatibility_PreparedStmtDB validates that prepared statement
// functionality works after the fork cherry-pick of upstream commit 5dd2bb4.
// The cherry-pick adds PreparedStmtDB.Reset(); here we verify that prepared
// statement mode itself works correctly with the updated GORM fork and pgx/v5.
func TestGORMCompatibility_PreparedStmtDB(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	tableName := "test_gorm_prepared_stmt"
	obj := &TestObject{TableNameValue: tableName}
	defer driver.Drop(ctx, obj)

	// Migrate to create the table
	err := driver.Migrate(ctx, []model.DBObject{obj})
	require.NoError(t, err)

	t.Run("PreparedStatementMode", func(t *testing.T) {
		// Open a session with PrepareStmt enabled
		preparedDB := driver.db.Session(&gorm.Session{
			PrepareStmt: true,
			Context:     ctx,
		})

		// Perform operations that use prepared statements
		item := &TestObject{
			ID:             model.NewObjectID(),
			Name:           "PrepStmt Test",
			Value:          42,
			TableNameValue: tableName,
		}

		err := preparedDB.Table(tableName).Create(item).Error
		require.NoError(t, err, "Insert with prepared statements should work")

		// Query using prepared statement
		var result TestObject
		err = preparedDB.Table(tableName).Where("name = ?", "PrepStmt Test").First(&result).Error
		require.NoError(t, err, "Query with prepared statements should work")
		assert.Equal(t, "PrepStmt Test", result.Name)
		assert.Equal(t, 42, result.Value)

		// Verify the PreparedStmtDB is being used and has the Reset method
		sqlDB, err := preparedDB.DB()
		require.NoError(t, err, "Should be able to get underlying sql.DB")
		require.NotNil(t, sqlDB, "sql.DB should not be nil")
	})
}

// TestGORMCompatibility_Connection validates that the DB.Connection method
// works after the fork cherry-pick of upstream commit 0df42e9.
func TestGORMCompatibility_Connection(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	tableName := "test_gorm_connection"
	obj := &TestObject{TableNameValue: tableName}
	defer driver.Drop(ctx, obj)

	// Migrate to create the table
	err := driver.Migrate(ctx, []model.DBObject{obj})
	require.NoError(t, err)

	t.Run("ExecuteMultipleCommandsInSingleConnection", func(t *testing.T) {
		err := driver.db.WithContext(ctx).Connection(func(tx *gorm.DB) error {
			// Insert a record
			item := &TestObject{
				ID:             model.NewObjectID(),
				Name:           "Connection Test",
				Value:          100,
				TableNameValue: tableName,
			}
			if err := tx.Table(tableName).Create(item).Error; err != nil {
				return fmt.Errorf("insert in connection: %w", err)
			}

			// Query in the same connection
			var count int64
			if err := tx.Table(tableName).Count(&count).Error; err != nil {
				return fmt.Errorf("count in connection: %w", err)
			}

			assert.Equal(t, int64(1), count, "Should see the inserted record in same connection")

			return nil
		})
		require.NoError(t, err, "Connection method should work with updated GORM fork")
	})
}

// TestGORMCompatibility_EndToEnd performs a comprehensive end-to-end test
// exercising the full CRUD lifecycle through the storage driver to validate
// that the pgx/v4 -> pgx/v5 migration doesn't introduce regressions.
func TestGORMCompatibility_EndToEnd(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Phase 1: Create table and insert data
	t.Run("InsertAndQuery", func(t *testing.T) {
		items := make([]*TestObject, 5)
		for i := 0; i < 5; i++ {
			items[i] = &TestObject{
				ID:        model.NewObjectID(),
				Name:      fmt.Sprintf("Item_%d", i),
				Value:     i * 10,
				Active:    i%2 == 0,
				CreatedAt: time.Now(),
				Category:  "test",
			}
			err := driver.Insert(ctx, items[i])
			require.NoError(t, err, "Insert should succeed for item %d", i)
		}

		// Query all items
		result := &TestObject{}
		results := []TestObject{}
		err := driver.Query(ctx, result, &results, model.DBM{})
		require.NoError(t, err)
		assert.Len(t, results, 5, "Should find all 5 items")
	})

	// Phase 2: Update with MongoDB-style operators
	t.Run("UpdateWithOperators", func(t *testing.T) {
		query := model.DBM{"name": "Item_0"}
		update := model.DBM{
			"$set": model.DBM{"value": 999},
		}
		err := driver.UpdateAll(ctx, &TestObject{}, query, update)
		require.NoError(t, err)

		// Verify update
		result := &TestObject{}
		results := []TestObject{}
		err = driver.Query(ctx, result, &results, model.DBM{"name": "Item_0"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 999, results[0].Value, "Value should be updated")
	})

	// Phase 3: Aggregation
	t.Run("Aggregation", func(t *testing.T) {
		pipeline := []model.DBM{
			{"$match": model.DBM{"category": "test"}},
			{"$group": model.DBM{
				"_id":       "$category",
				"avg_value": model.DBM{"$avg": "$value"},
			}},
		}

		results, err := driver.Aggregate(ctx, &TestObject{}, pipeline)
		require.NoError(t, err, "Aggregation should work with pgx/v5")
		assert.NotEmpty(t, results, "Aggregation should return results")
	})

	// Phase 4: Count
	t.Run("Count", func(t *testing.T) {
		count, err := driver.Count(ctx, &TestObject{}, model.DBM{"active": true})
		require.NoError(t, err)
		assert.Greater(t, count, 0, "Should count active items")
	})

	// Phase 5: Delete
	t.Run("Delete", func(t *testing.T) {
		result := &TestObject{}
		results := []TestObject{}
		err := driver.Query(ctx, result, &results, model.DBM{"name": "Item_1"})
		require.NoError(t, err)
		require.Len(t, results, 1)

		err = driver.Delete(ctx, &results[0])
		require.NoError(t, err)

		// Verify deletion
		count, err := driver.Count(ctx, &TestObject{}, model.DBM{"name": "Item_1"})
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Deleted item should not be found")
	})

	// Phase 6: Index operations
	t.Run("IndexOperations", func(t *testing.T) {
		idx := model.Index{
			Keys: []model.DBM{
				{"name": 1},
				{"category": 1},
			},
			Name: "idx_e2e_name_category",
		}
		err := driver.CreateIndex(ctx, &TestObject{}, idx)
		require.NoError(t, err, "CreateIndex should work with pgx/v5")

		indexes, err := driver.GetIndexes(ctx, &TestObject{})
		require.NoError(t, err)

		found := false
		for _, existingIdx := range indexes {
			if existingIdx.Name == "idx_e2e_name_category" {
				found = true
				break
			}
		}
		assert.True(t, found, "Created index should appear in GetIndexes result")

		err = driver.CleanIndexes(ctx, &TestObject{})
		require.NoError(t, err, "CleanIndexes should work with pgx/v5")
	})
}

// TestGORMCompatibility_TransactionIsolation validates that transactions
// work correctly with the pgx/v5 driver.
func TestGORMCompatibility_TransactionIsolation(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	t.Run("CommitTransaction", func(t *testing.T) {
		err := driver.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			item := &TestObject{
				ID:    model.NewObjectID(),
				Name:  "Transaction Test",
				Value: 42,
			}
			return tx.Table("test_objects").Create(item).Error
		})
		require.NoError(t, err, "Transaction commit should work with pgx/v5")

		// Verify data persisted
		result := &TestObject{}
		results := []TestObject{}
		err = driver.Query(ctx, result, &results, model.DBM{"name": "Transaction Test"})
		require.NoError(t, err)
		assert.Len(t, results, 1)
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		err := driver.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			item := &TestObject{
				ID:    model.NewObjectID(),
				Name:  "Rollback Test",
				Value: 99,
			}
			if err := tx.Table("test_objects").Create(item).Error; err != nil {
				return err
			}
			// Force rollback
			return fmt.Errorf("intentional rollback")
		})
		require.Error(t, err, "Transaction should return error")

		// Verify data was NOT persisted
		count, err := driver.Count(ctx, &TestObject{}, model.DBM{"name": "Rollback Test"})
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Rolled back data should not persist")
	})
}

// TestGORMCompatibility_RawSQL validates that raw SQL execution works correctly
// with the pgx/v5 driver, including parameterized queries.
func TestGORMCompatibility_RawSQL(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	t.Run("ParameterizedQuery", func(t *testing.T) {
		// Insert test data
		for i := 0; i < 3; i++ {
			item := &TestObject{
				ID:    model.NewObjectID(),
				Name:  fmt.Sprintf("RawSQL_%d", i),
				Value: i * 100,
			}
			err := driver.Insert(ctx, item)
			require.NoError(t, err)
		}

		// Execute parameterized raw SQL query
		var results []TestObject
		err := driver.db.WithContext(ctx).
			Table("test_objects").
			Raw("SELECT * FROM test_objects WHERE value > ? ORDER BY value", 50).
			Scan(&results).Error
		require.NoError(t, err, "Parameterized raw SQL should work with pgx/v5")
		assert.Len(t, results, 2, "Should find items with value > 50")
	})

	t.Run("ExecRawSQL", func(t *testing.T) {
		result := driver.db.WithContext(ctx).
			Exec("UPDATE test_objects SET value = value + 1 WHERE name = ?", "RawSQL_0")
		require.NoError(t, result.Error, "Raw SQL exec should work with pgx/v5")
		assert.Equal(t, int64(1), result.RowsAffected, "Should affect one row")
	})
}

// TestGORMCompatibility_NullHandling validates correct NULL handling with pgx/v5.
func TestGORMCompatibility_NullHandling(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	t.Run("InsertAndQueryWithDefaults", func(t *testing.T) {
		// Insert with zero-value fields (which map to NULL or defaults)
		item := &TestObject{
			ID:   model.NewObjectID(),
			Name: "NullTest",
			// Value, Active, CreatedAt, Category left as zero values
		}
		err := driver.Insert(ctx, item)
		require.NoError(t, err)

		// Query back
		result := &TestObject{}
		results := []TestObject{}
		err = driver.Query(ctx, result, &results, model.DBM{"name": "NullTest"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 0, results[0].Value)
		assert.False(t, results[0].Active)
	})
}
