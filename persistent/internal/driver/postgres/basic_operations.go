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
	"time"
)

func (d *driver) Insert(ctx context.Context, objects ...model.DBObject) error {
	if d.db == nil {
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
		if err := d.db.WithContext(ctx).Table(tableName).Create(sliceValue.Interface()).Error; err != nil {
			return err
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
	result := tx.Save(object)
	if result.Error != nil {
		return result.Error
	}

	// Check if any rows were affected
	if result.RowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// createTempTableWithMatchingTypes creates a temporary table with the same column types as the main table
func (d *driver) createTempTableWithMatchingTypes(tx *gorm.DB, tableName string, fields map[string]bool) (string, error) {
	// Generate a unique temporary table name
	tempTableName := fmt.Sprintf("temp_bulk_update_%v", time.Now().UnixNano())

	// Query the database schema to get column types
	var columnInfo []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}

	schemaQuery := `
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = ? AND column_name != 'id'
    `

	if err := tx.Raw(schemaQuery, tableName).Scan(&columnInfo).Error; err != nil {
		return "", fmt.Errorf("failed to get column types: %w", err)
	}

	// Create a map of column names to their data types
	columnTypes := make(map[string]string)
	for _, col := range columnInfo {
		columnTypes[col.ColumnName] = col.DataType
	}

	// Build CREATE TEMPORARY TABLE statement with matching column types
	createTempTableSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (id TEXT PRIMARY KEY", tempTableName)
	for field := range fields {
		dataType, exists := columnTypes[field]
		if !exists {
			// If we don't have type info, default to TEXT
			dataType = "TEXT"
		}
		createTempTableSQL += fmt.Sprintf(", %s %s", field, dataType)
	}
	createTempTableSQL += ")"

	// Create the temporary table using GORM's Exec
	if err := tx.Exec(createTempTableSQL).Error; err != nil {
		return "", err
	}

	return tempTableName, nil
}

// insertDataIntoTempTable inserts data from objects into the temporary table
func (d *driver) insertDataIntoTempTable(tx *gorm.DB, tempTableName string, objects []model.DBObject, fields map[string]bool) error {
	if len(objects) > 0 {
		// Build column list
		columns := []string{"id"}
		for field := range fields {
			columns = append(columns, field)
		}

		// Start building the INSERT statement
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
			tempTableName, strings.Join(columns, ", "))

		// Build value placeholders and args
		placeholders := make([]string, 0, len(objects))
		valueArgs := make([]interface{}, 0, len(objects)*(len(columns)))

		for _, obj := range objects {
			data, err := objectToMap(obj)
			if err != nil {
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
			}

			if idValue == "" {
				continue // Skip objects without ID
			}

			// Create placeholders for this row
			rowPlaceholders := make([]string, 0, len(columns))
			rowPlaceholders = append(rowPlaceholders, "?")
			valueArgs = append(valueArgs, idValue)

			for _, field := range columns[1:] { // Skip ID column
				rowPlaceholders = append(rowPlaceholders, "?")
				val, ok := data[field]
				if !ok {
					val = nil
				}
				valueArgs = append(valueArgs, val)
			}

			placeholders = append(placeholders,
				fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))

		}

		// Complete the INSERT statement
		insertSQL += strings.Join(placeholders, ", ")

		// Execute the bulk insert
		if err := tx.Exec(insertSQL, valueArgs...).Error; err != nil {
			return err
		}
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
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}
	if len(objects) == 0 {
		return errors.New(types.ErrorEmptyRow)
	}
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
	db := d.db.WithContext(ctx).Table(tableName)

	// Check if query is empty
	hasFilter := false
	for k := range query {
		if !strings.HasPrefix(k, "_") && k != "$or" { // Skip special keys
			hasFilter = true
			break
		}
	}

	if hasFilter {
		db = d.translateQuery(db, query, row)
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

func (d *driver) Upsert1(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Check if the database connection is valid
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	// ensure ID
	if row.GetObjectID() == "" {
		row.SetObjectID(model.NewObjectID())
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

	// Save the original ID to ensure it's preserved
	originalID := row.GetObjectID()

	updateDB := tx.Table(tableName)
	updateDB = d.translateQuery(updateDB, query, row)

	// Get the update map from MongoDB operators
	_, updateMap, err := d.applyMongoUpdateOperators(updateDB, update)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Execute the update
	result := updateDB.Updates(updateMap)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	// Check if any rows were affected
	if result.RowsAffected > 0 {
		// Update succeeded, fetch the updated document
		fetchDB := tx.Table(tableName)
		fetchDB = d.translateQuery(fetchDB, query, row)
		err = fetchDB.First(row).Error
		if err != nil {
			tx.Rollback()
			return err
		}

		// Ensure the original ID is preserved
		if originalID != "" {
			row.SetObjectID(originalID)
		}
	} else {
		// No rows were affected, perform an insert
		// Use the original ID if provided
		if originalID != "" {
			row.SetObjectID(originalID)
		} else {
			// Generate a new ID if not provided
			row.SetObjectID(model.NewObjectID())
		}

		// Create a new instance of the same type as row
		newRow := reflect.New(reflect.TypeOf(row).Elem()).Interface().(model.DBObject)
		newRow.SetObjectID(row.GetObjectID())

		// Apply query fields to the new row
		for k, v := range query {
			if !strings.HasPrefix(k, "_") && k != "$or" { // Skip special keys
				setField(newRow, k, v)
			}
		}

		// Apply update fields to the new row
		if raw, ok := update["$set"]; ok {
			switch setMap := raw.(type) {
			case map[string]interface{}:
				for k, v := range setMap {
					setField(newRow, k, v)
				}
			case model.DBM:
				for k, v := range setMap {
					setField(newRow, k, v)
				}
			}
		}

		// Insert the new row
		result := tx.Table(tableName).Create(newRow)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}

		// Copy values from newRow to row
		copyStructValues(newRow, row)
		// Ensure the original ID is preserved
		if originalID != "" {
			row.SetObjectID(originalID)
		}

	}
	return tx.Commit().Error
}

func (d *driver) Upsert(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Validate DB + table
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	tx := d.db.WithContext(ctx).Begin()
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

	// Apply query
	updateDB := tx.Table(tableName)
	updateDB = d.translateQuery(updateDB, query, row)

	// Translate Mongo-style update operators
	_, updateMap, err := d.applyMongoUpdateOperators(updateDB, update)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Attempt update
	result := updateDB.Updates(updateMap)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	if result.RowsAffected > 0 {
		// Existing row updated → fetch updated values
		fetchDB := tx.Table(tableName)
		fetchDB = d.translateQuery(fetchDB, query, row)
		err = fetchDB.First(row).Error
		if err != nil {
			tx.Rollback()
			return err
		}
		if originalID != "" {
			row.SetObjectID(originalID)
		}
		return tx.Commit().Error
	}

	// -------------------
	// Insert path
	// -------------------

	// Ensure ID
	if originalID != "" {
		row.SetObjectID(originalID)
	} else if qid, ok := query["id"]; ok {
		if sid, ok2 := qid.(string); ok2 && sid != "" {
			row.SetObjectID(model.ObjectIDHex(sid))

		}
	}
	if row.GetObjectID() == "" {
		row.SetObjectID(model.NewObjectID())
	}

	// New instance of row type
	newRow := reflect.New(reflect.TypeOf(row).Elem()).Interface().(model.DBObject)
	newRow.SetObjectID(row.GetObjectID())

	// Apply query fields to newRow
	for k, v := range query {
		if !strings.HasPrefix(k, "_") && k != "$or" {
			setField(newRow, k, v)
		}
	}

	// Apply update fields to newRow
	applySetOperatorToObject(newRow, update)

	// Insert
	result = tx.Table(tableName).Create(newRow)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	// Copy values into caller row
	copyStructValues(newRow, row)

	// Preserve original ID if given
	if originalID != "" {
		row.SetObjectID(originalID)
	}

	return tx.Commit().Error
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
