package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"strings"
	"time"
)

func (d *driver) Insert(ctx context.Context, objects ...model.DBObject) error {
	// Check if the database connection is valid
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	for _, obj := range objects {
		// Generate a new ID if not set
		if obj.GetObjectID() == "" {
			obj.SetObjectID(model.NewObjectID())
		}

		result := d.db.WithContext(ctx).Table(obj.TableName()).Create(obj)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func (d *driver) Delete(ctx context.Context, object model.DBObject, filters ...model.DBM) error {
	tableName, err := d.validateDBAndTable(object)
	if err != nil {
		return err
	}

	// Check if we have multiple filters
	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	// Start building the query with the table name
	db := d.db.WithContext(ctx).Table(tableName)
	// If we have a filter, use our translator function
	if len(filters) == 1 {
		db = d.translateQuery(db, filters[0], object)
	} else {
		// If no filter is provided, use the object's ID as the filter
		id := object.GetObjectID()
		if id != "" {
			db = db.Where("id = ?", id.Hex())
		} else {
			// No filter and no ID, nothing to delete
			return nil
		}
	}
	// Execute the DELETE operation
	result := db.Delete(object)
	if result.Error != nil {
		return result.Error
	}

	// Check if any rows were affected
	if result.RowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (d *driver) Update(ctx context.Context, object model.DBObject, filters ...model.DBM) error {
	tableName, err := d.validateDBAndTable(object)
	if err != nil {
		return err
	}

	// Check if we have multiple filters
	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	// Convert DBObject to map for updating
	data, err := objectToMap(object)
	if err != nil {
		return err
	}

	// Remove the ID field from the update data if it exists
	// as we typically don't want to update the primary key
	delete(data, "_id")
	delete(data, "id")

	if len(data) == 0 {
		// Nothing to update
		return nil
	}

	// Start building the query with the table name
	tx := d.db.WithContext(ctx).Table(tableName)
	// If we have a filter, use our translator function
	if len(filters) == 1 {
		tx = d.translateQuery(tx, filters[0], object)
	} else {
		// If no filter is provided, use the object's ID as the filter
		id := object.GetObjectID()
		if id != "" {
			tx = tx.Where("id = ?", id.Hex())
		} else {
			return errors.New("no filter provided and object has no ID")
		}
	}

	// Execute the UPDATE operation
	result := tx.Updates(data)
	if result.Error != nil {
		return result.Error
	}

	// Check if any rows were affected
	if result.RowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

/*
BulkUpdate function is designed to efficiently update multiple objects in the database.
It has two main operational modes:
- With Filter: When a filter is provided, it updates all records matching the filter with the values from the provided objects.
This is useful for batch updates where multiple records need to be updated based on a common condition.
- Without Filter: When no filter is provided, it updates each object individually based on its ID. This is useful
for updating a collection of specific records with different values.
*/
func (d *driver) BulkUpdate(ctx context.Context, objects []model.DBObject, filters ...model.DBM) error {
	// Check if the database connection is valid
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Check if we have objects to update
	if len(objects) == 0 {
		return errors.New(types.ErrorEmptyRow)
	}
	// Check if we have multiple filters
	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	// Start a transaction
	tx := d.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r) // re-throw panic after rollback
		}
	}()

	/*
	   If a filter is provided, use it for all objects
	   When a filter is provided, we'll use GORM's transaction and raw SQL
	   capabilities to implement the temporary table approach for efficiency.
	*/
	if len(filters) == 1 {
		if len(objects) == 0 {
			return errors.New(types.ErrorEmptyRow)
		}

		tableName := objects[0].TableName()
		if tableName == "" {
			return errors.New(types.ErrorEmptyTableName)
		}

		// Create a temporary table to hold the update data
		tempTableName := fmt.Sprintf("temp_bulk_update_%v", time.Now().UnixNano())

		// Get all field names from all objects
		allFields := make(map[string]bool)
		for _, obj := range objects {
			data, err := objectToMap(obj)
			if err != nil {
				tx.Rollback()
				return err
			}

			for k := range data {
				if k != "_id" && k != "id" {
					allFields[k] = true
				}
			}
		}
		// Build CREATE TEMPORARY TABLE statement
		createTempTableSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (id TEXT PRIMARY KEY", tempTableName)
		for field := range allFields {
			createTempTableSQL += fmt.Sprintf(", %s TEXT", field)
		}
		createTempTableSQL += ")"
		// Create the temporary table using GORM's Exec
		if err := tx.Exec(createTempTableSQL).Error; err != nil {
			tx.Rollback()
			return err
		}
		// Insert data into the temporary table
		for _, obj := range objects {
			data, err := objectToMap(obj)
			if err != nil {
				tx.Rollback()
				return err
			}

			// Get the ID
			var idValue string
			if id, ok := data["_id"]; ok {
				idValue = fmt.Sprintf("%v", id)
			} else if id, ok := data["id"]; ok {
				idValue = fmt.Sprintf("%v", id)
			} else if id := obj.GetObjectID(); id != "" {
				idValue = id.Hex()
			} else {
				continue // Skip objects without ID
			}

			// Prepare data for insertion
			insertData := map[string]interface{}{
				"id": idValue,
			}

			for field := range allFields {
				if val, ok := data[field]; ok {
					insertData[field] = fmt.Sprintf("%v", val)
				} else {
					insertData[field] = nil
				}
			}

			// Insert into temporary table
			if err := tx.Table(tempTableName).Create(insertData).Error; err != nil {
				tx.Rollback()
				return err
			}
		}

		// Build the UPDATE statement using the temporary table
		updateSQL := fmt.Sprintf("UPDATE %s SET ", tableName)
		setClauses := make([]string, 0, len(allFields))
		for field := range allFields {
			setClauses = append(setClauses, fmt.Sprintf("%s = temp.%s", field, field))
		}

		updateSQL += strings.Join(setClauses, ", ")
		updateSQL += fmt.Sprintf(" FROM %s AS temp WHERE %s.id = temp.id", tempTableName, tableName)

		// Add the filter condition if provided
		if len(filters) == 1 {
			filter := filters[0]
			db := d.db.WithContext(ctx).Table(tableName)
			db = d.translateQuery(db, filter, objects[0])

			whereConditions := []string{}
			whereArgs := []interface{}{}

			for k, v := range filter {
				if !strings.HasPrefix(k, "_") && k != "$or" { // Skip special keys
					whereConditions = append(whereConditions, fmt.Sprintf("%s = ?", k))
					whereArgs = append(whereArgs, v)
				}
			}
			if len(whereConditions) > 0 {
				updateSQL += " AND " + strings.Join(whereConditions, " AND ")

				// Execute the update with the filter
				if err := tx.Exec(updateSQL, whereArgs...).Error; err != nil {
					tx.Rollback()
					return err
				} else {
					// Execute the update without additional filters
					if err := tx.Exec(updateSQL).Error; err != nil {
						tx.Rollback()
						return err
					}
				}
				// Drop the temporary table
				if err := tx.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName)).Error; err != nil {
					tx.Rollback()
					return err
				}
			} else {
				// Execute the update without additional filters
				if err := tx.Exec(updateSQL).Error; err != nil {
					tx.Rollback()
					return err
				}
			}
		}
	} else {
		for _, obj := range objects {
			// Convert object to map
			data, err := objectToMap(obj)
			if err != nil {
				tx.Rollback()
				return err
			}

			// Get the table name
			tableName := obj.TableName()

			// Remove ID fields from update data
			delete(data, "_id")
			delete(data, "id")

			if len(data) == 0 {
				continue // Nothing to update
			}

			// Get the object ID for the WHERE clause
			id := obj.GetObjectID()
			if id == "" {
				continue // Skip objects without ID
			}

			// Update using GORM
			if err := tx.Table(tableName).Where("id = ?", id.Hex()).Updates(data).Error; err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	// Commit the transaction
	return tx.Commit().Error
}

func (d *driver) UpdateAll(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	// Check if update is empty
	if len(update) == 0 {
		return nil // Nothing to update
	}

	db := d.db.WithContext(ctx).Table(tableName)
	// Apply the query filter
	db = d.translateQuery(db, query, row)

	// Apply MongoDB update operators
	db, err = d.applyMongoUpdateOperators(db, update)
	if err != nil {
		return err
	}

	return nil
}

func (d *driver) Upsert(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Check if the database connection is valid
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	// Start a transaction
	tx := d.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r) // re-throw panic after rollback
		}
	}()

	db := tx.Table(tableName)
	db = d.translateQuery(db, query, row)

	// Apply MongoDB update operators
	db, err = d.applyMongoUpdateOperators(db, update)
	if err != nil {
		tx.Rollback()
		return err
	}

	if db.RowsAffected == 0 {
		// No rows were affected, perform an insert
		// Merge query and update for the insert
		insertData := model.DBM{}

		// Add query fields
		for k, v := range query {
			insertData[k] = v
		}
		// Add update fields
		if setMap, ok := update["$set"].(map[string]interface{}); ok {
			for k, v := range setMap {
				insertData[k] = v
			}
		} else {
			// If no $set operator, use the update directly
			for k, v := range update {
				if !strings.HasPrefix(k, "$") {
					insertData[k] = v
				}
			}
		}

		// Generate a new ID if not provided
		if _, hasID := insertData["_id"]; !hasID && row.GetObjectID() == "" {
			row.SetObjectID(model.NewObjectID())
			insertData["_id"] = row.GetObjectID()
		}

		// Insert the new record
		result := tx.Table(tableName).Create(insertData)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}
		// Fetch the inserted record to populate the row object
		result = tx.Table(tableName).Where("id = ?", row.GetObjectID().Hex()).First(row)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}
	} else {
		// Update succeeded, get the updated row
		result := tx.Table(tableName)
		result = d.translateQuery(result, query, row)
		result = result.First(row)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}
	}

	return tx.Commit().Error
}
