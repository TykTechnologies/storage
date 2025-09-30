package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

// Insert adds one or more objects into the database in a single batch operation.
// Returns an error if the input is empty or the insert fails.
func (d *driver) Insert(ctx context.Context, objects ...model.DBObject) error {
	if d.writeDB == nil {
		return errors.New(types.ErrorSessionClosed)
	}
	if len(objects) == 0 {
		return nil
	}

	// Group objects by table + concrete type
	typeKey := func(obj model.DBObject) string {
		return fmt.Sprintf("%s|%T", obj.TableName(), obj)
	}
	batches := make(map[string][]model.DBObject)

	for _, obj := range objects {
		if obj.GetObjectID() == "" {
			obj.SetObjectID(model.NewObjectID())
		}
		key := typeKey(obj)
		batches[key] = append(batches[key], obj)
	}

	// Insert each homogeneous batch
	for _, objs := range batches {
		if len(objs) == 0 {
			continue
		}

		// reflect to build []T where T is the concrete type
		first := objs[0]
		sliceType := reflect.SliceOf(reflect.TypeOf(first)) // []*MyStruct
		sliceValue := reflect.MakeSlice(sliceType, 0, len(objs))

		for _, obj := range objs {
			sliceValue = reflect.Append(sliceValue, reflect.ValueOf(obj))
		}

		tableName := objs[0].TableName()
		if err := d.writeDB.WithContext(ctx).Table(tableName).Create(sliceValue.Interface()).Error; err != nil {
			return err
		}
	}
	return nil
}

// Delete removes the specified object from the database using either the provided filter
// or the object's ID. Returns an error if no rows are affected.
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
	db := d.writeDB.WithContext(ctx).Table(tableName)
	// If we have a filter, use our translator function
	if len(filters) == 1 {
		db, err = d.translateQuery(db, filters[0], object)
		if err != nil {
			return err
		}
	} else if object.GetObjectID() != "" {
		// If no filter is provided, use the object's ID as the filter
		id := object.GetObjectID()
		db = db.Where("id = ?", id.Hex())
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

// Update applies changes from the given object to the database, using either the provided filter
// or the object's ID. Excludes ID fields and returns an error if no rows are affected.
func (d *driver) Update(ctx context.Context, object model.DBObject, filters ...model.DBM) error {
	tableName, err := d.validateDBAndTable(object)
	if err != nil {
		return err
	}

	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	tx := d.writeDB.WithContext(ctx).Table(tableName)

	// Apply filters
	if len(filters) == 1 {
		tx, err = d.translateQuery(tx, filters[0], object)
		if err != nil {
			return err
		}
	} else {
		id := object.GetObjectID()
		if id != "" {
			tx = tx.Where("id = ?", id.Hex())
		} else {
			return errors.New("no filter provided and object has no ID")
		}
	}

	// Save replaces all fields with the object’s values
	result := tx.Save(object)
	if result.Error != nil {
		return result.Error
	}
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
	// Basic validation
	if d.writeDB == nil {
		return errors.New(types.ErrorSessionClosed)
	}
	if len(objects) == 0 {
		return errors.New(types.ErrorEmptyRow)
	}
	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	// Start a transaction
	tx := d.writeDB.WithContext(ctx).Begin()
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

	// Get the table name
	tableName := objects[0].TableName()
	if tableName == "" {
		tx.Rollback()
		return errors.New(types.ErrorEmptyTableName)
	}

	// For filter-based updates, we only need the first object's values
	// For ID-based updates, we'll process each object separately
	if len(filters) == 1 {
		// Extract update values from the first object
		updateData, err := objectToMap(objects[0])
		if err != nil {
			tx.Rollback()
			return err
		}

		// Remove ID fields from update data
		delete(updateData, "_id")
		delete(updateData, "id")

		if len(updateData) == 0 {
			tx.Rollback()
			return nil // Nothing to update
		}

		// Build the query with the filter
		query := tx.Table(tableName)
		filter := filters[0]

		for k, v := range filter {
			if !strings.HasPrefix(k, "_") && k != "$or" { // Skip special keys
				query = query.Where(k+" = ?", v)
			}
		}

		// Execute the update directly
		result := query.Updates(updateData)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}
	} else {
		// No filter provided, update each object by ID
		for _, obj := range objects {
			// Get the object ID
			id := obj.GetObjectID()
			if id == "" {
				continue // Skip objects without ID
			}

			// Extract update values
			updateData, err := objectToMap(obj)
			if err != nil {
				tx.Rollback()
				return err
			}

			// Remove ID fields from update data
			delete(updateData, "_id")
			delete(updateData, "id")

			if len(updateData) == 0 {
				continue // Nothing to update
			}

			// Update by ID
			result := tx.Table(tableName).Where("id = ?", id.Hex()).Updates(updateData)
			if result.Error != nil {
				tx.Rollback()
				return result.Error
			}

			// Check if any rows were affected
			if result.RowsAffected == 0 {
				tx.Rollback()
				return sql.ErrNoRows
			}
		}
	}

	// Commit the transaction
	return tx.Commit().Error
}

// UpdateAll updates all rows in the database matching the given query with the provided update values.
// Returns an error if the operation fails or the inputs are invalid.
func (d *driver) UpdateAll(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	// Check if update is empty
	if len(update) == 0 {
		return nil // Nothing to update
	}

	// Start a transaction
	tx := d.writeDB.WithContext(ctx).Begin()
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
	db := d.writeDB.WithContext(ctx).Table(tableName)

	// Check if query is empty
	hasFilter := false
	for k := range query {
		if !strings.HasPrefix(k, "_") && k != "$or" { // Skip special keys
			hasFilter = true
			break
		}
	}

	if hasFilter {
		db, err = d.translateQuery(db, query, row)
		if err != nil {
			tx.Rollback()
			return err
		}
	} else {
		// Empty query means update all documents
		// Use a session that allows global updates
		db = db.Session(&gorm.Session{AllowGlobalUpdate: true})
	}

	// Get the update map from MongoDB operators
	_, updateMap, err := d.applyMongoUpdateOperators(db, update)
	if err != nil {
		tx.Rollback()
		return err
	}

	if len(updateMap) == 0 {
		tx.Rollback()
		return nil // Nothing to update
	}

	// Execute the update with the built updateMap
	result := db.Updates(updateMap)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	// Check if any rows were affected
	if result.RowsAffected == 0 {
		tx.Rollback()
		return sql.ErrNoRows
	}

	// Commit the transaction
	return tx.Commit().Error
}

func (d *driver) Upsert(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Validate DB + table
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	tx := d.writeDB.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	originalID := row.GetObjectID()

	// -------------------
	// Attempt update
	// -------------------
	updateDB := tx.Table(tableName)
	updateDB, err = d.translateQuery(updateDB, query, row)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, updateMap, err := d.applyMongoUpdateOperators(updateDB, update)
	if err != nil {
		tx.Rollback()
		return err
	}

	result := updateDB.Updates(updateMap)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	if result.RowsAffected > 0 {
		if err := d.fetchUpdatedRow(tx, tableName, query, row); err != nil {
			tx.Rollback()
			return err
		}
		// Preserve original ID
		if originalID != "" {
			row.SetObjectID(originalID)
		}
		return tx.Commit().Error
	}

	// -------------------
	// Insert path
	// -------------------
	ensureID(originalID, row, query)

	// Create a new instance for insertion
	newRow := cloneDBObject(row)

	// Merge query fields into newRow
	mergeQueryFields(newRow, query)

	// Apply update fields to newRow
	applySetOperatorToObject(newRow, update)

	// Insert
	if err := tx.Table(tableName).Create(newRow).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Copy back values to caller row
	copyStructValues(newRow, row)

	// Preserve original ID
	if originalID != "" {
		row.SetObjectID(originalID)
	}

	return tx.Commit().Error
}

func (d *driver) fetchUpdatedRow(tx *gorm.DB, table string, query model.DBM, row model.DBObject) error {
	db := tx.Table(table)
	db, err := d.translateQuery(db, query, row)
	if err != nil {
		return err
	}
	return db.First(row).Error
}

// Helper function to set a field in a struct using reflection
func setField(obj interface{}, name string, value interface{}) {
	structValue := reflect.ValueOf(obj)
	if structValue.Kind() != reflect.Ptr {
		return
	}

	structElem := structValue.Elem()
	if structElem.Kind() != reflect.Struct {
		return
	}

	// Convert snake_case to CamelCase
	fieldName := strings.Replace(strings.Title(strings.Replace(name, "_", " ", -1)), " ", "", -1)

	field := structElem.FieldByName(fieldName)
	if !field.IsValid() || !field.CanSet() {
		return
	}

	valueVal := reflect.ValueOf(value)

	// Try to set the field value
	if valueVal.Type().AssignableTo(field.Type()) {
		field.Set(valueVal)
	} else if valueVal.Type().ConvertibleTo(field.Type()) {
		field.Set(valueVal.Convert(field.Type()))
	}
}

// Helper function to copy values from one struct to another
func copyStructValues(src, dst interface{}) {
	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)

	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}

	if dstVal.Kind() == reflect.Ptr {
		dstVal = dstVal.Elem()
	}

	if srcVal.Kind() != reflect.Struct || dstVal.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < srcVal.NumField(); i++ {
		srcField := srcVal.Field(i)
		fieldName := srcVal.Type().Field(i).Name

		dstField := dstVal.FieldByName(fieldName)
		if dstField.IsValid() && dstField.CanSet() {
			if srcField.Type().AssignableTo(dstField.Type()) {
				dstField.Set(srcField)
			} else if srcField.Type().ConvertibleTo(dstField.Type()) {
				dstField.Set(srcField.Convert(dstField.Type()))
			}
		}
	}
}

// helper: apply $set contents of update to object
func applySetOperatorToObject(obj model.DBObject, update model.DBM) {
	if raw, ok := update["$set"]; ok {
		switch setMap := raw.(type) {
		case map[string]interface{}:
			for k, v := range setMap {
				setField(obj, k, v)
			}
		case model.DBM:
			for k, v := range setMap {
				setField(obj, k, v)
			}
		}
	}
}
