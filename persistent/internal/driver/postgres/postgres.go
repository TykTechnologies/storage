package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/TykTechnologies/storage/persistent/utils"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var _ types.PersistentStorage = &driver{}

type driver struct {
	db     *sql.DB
	dbName string
}

func (p *driver) Insert(ctx context.Context, objects ...model.DBObject) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	for _, obj := range objects {
		// Generate a new ID if not set
		if obj.GetObjectID() == "" {
			obj.SetObjectID(model.NewObjectID())
		}

		// Convert DBObject to map for insertion
		data, err := objectToMap(obj)
		if err != nil {
			return err
		}

		// Build SQL INSERT statement
		tableName := obj.TableName()
		columns := make([]string, 0, len(data))
		placeholders := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data))

		i := 1
		for k, v := range data {
			columns = append(columns, k)
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			values = append(values, v)
			i++
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		// Execute the query
		_, err = p.db.ExecContext(ctx, query, values...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *driver) Delete(ctx context.Context, object model.DBObject, filters ...model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := object.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Check if we have multiple filters
	if len(filters) > 1 {
		return errors.New(types.ErrorMultipleDBM)
	}

	// Build the SQL DELETE statement
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	var args []interface{}

	// If we have a filter, add a WHERE clause
	if len(filters) == 1 {
		filter := filters[0]
		whereClause, whereArgs := buildWhereClause(filter)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	} else {
		// If no filter is provided, use the object's ID as the filter
		id := object.GetObjectID()
		if id != "" {
			query += " WHERE id = $1"
			args = []interface{}{id.Hex()}
		}
	}

	// Execute the DELETE statement
	result, err := p.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (p *driver) Update(ctx context.Context, object model.DBObject, filters ...model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := object.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
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

	// Build the SQL UPDATE statement
	setClauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	i := 1
	for k, v := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", k, i))
		values = append(values, v)
		i++
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s",
		tableName,
		strings.Join(setClauses, ", "),
	)

	// Add WHERE clause if filter is provided
	if len(filters) == 1 {
		filter := filters[0]
		whereClause, whereArgs := buildWhereClause(filter)
		if whereClause != "" {
			query += " WHERE " + whereClause
			values = append(values, whereArgs...)
		}
	} else {
		// If no filter is provided, use the object's ID as the filter
		id := object.GetObjectID()
		if id != "" {
			query += fmt.Sprintf(" WHERE id = $%d", i)
			values = append(values, id.Hex())
		} else {
			return errors.New("no filter provided and object has no ID")
		}
	}

	// Execute the UPDATE statement
	result, err := p.db.ExecContext(ctx, query, values...)
	if err != nil {
		return err
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	// If no rows were affected, it might be considered an error in some cases
	// Depending on the application's requirements
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (p *driver) Count(ctx context.Context, row model.DBObject, filters ...model.DBM) (count int, error error) {
	// Check if the database connection is valid
	if p.db == nil {
		return 0, errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return 0, errors.New(types.ErrorEmptyTableName)
	}

	// Check if we have multiple filters
	if len(filters) > 1 {
		return 0, errors.New(types.ErrorMultipleDBM)
	}

	// Build the SQL COUNT query
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

	var args []interface{}
	// Add WHERE clause if filter is provided
	if len(filters) == 1 {
		filter := filters[0]
		whereClause, whereArgs := buildWhereClause(filter)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	}

	// Execute the query
	var rowCount int
	err := p.db.QueryRowContext(ctx, query, args...).Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (p *driver) Query(ctx context.Context, object model.DBObject, result interface{}, filter model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	tableName := object.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	whereClause, values := buildWhereClause(filter)

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	// Execute the query
	rows, err := p.db.QueryContext(ctx, query, values...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Process results
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		return errors.New("result must be a pointer")
	}

	// Check if result is a slice or a single object
	resultElem := resultVal.Elem()
	isSingle := resultElem.Kind() != reflect.Slice

	if isSingle {
		// Single object result
		if !rows.Next() {
			return errors.New("no rows found")
		}
		err = scanRowToObject(rows, columns, resultElem.Interface())
		if err != nil {
			return err
		}
	} else {
		sliceType := resultElem.Type().Elem()
		for rows.Next() {
			// Create a new object of the slice element type
			newObj := reflect.New(sliceType).Elem()

			err = scanRowToObject(rows, columns, newObj.Addr().Interface())
			if err != nil {
				return err
			}

			// Append to the result slice
			resultElem.Set(reflect.Append(resultElem, newObj))
		}
	}
	return rows.Err()
}

func (p *driver) BulkUpdate(ctx context.Context, objects []model.DBObject, filters ...model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
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
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	/*
		If a filter is provided, use it for all objects
		When a filter is provided, it creates a temporary table to hold the update data,
		inserts the new values for each object, and then performs a single UPDATE with a
		JOIN to update all matching rows in the main table.
	*/
	if len(filters) == 1 {

		// Get the common table name (assuming all objects are of the same type)
		if len(objects) == 0 {
			return errors.New(types.ErrorEmptyRow)
		}
		tableName := objects[0].TableName()
		if tableName == "" {
			return errors.New(types.ErrorEmptyTableName)
		}

		// Create a temporary table to hold the update data
		tempTableName := fmt.Sprintf("temp_bulk_update_%p", time.Now().UnixNano())
		createTempTableSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (id TEXT PRIMARY KEY", tempTableName)

		// Get all field names from all objects
		allFields := make(map[string]bool)
		for _, obj := range objects {
			data, err := objectToMap(obj)
			if err != nil {
				return err
			}
			for k := range data {
				if k != "_id" && k != "id" {
					allFields[k] = true
				}
			}
		}

		// Add columns to the temporary table
		for field := range allFields {
			createTempTableSQL += fmt.Sprintf(", %s TEXT", field)
		}

		createTempTableSQL += ")"
		// Create the temporary table
		_, err = tx.ExecContext(ctx, createTempTableSQL)
		if err != nil {
			return err
		}

		// Insert data into the temporary table
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
			} else {
				continue // Skip objects without ID
			}

			// Build the INSERT statement
			columns := []string{"id"}
			placeholders := []string{"$1"}
			values := []interface{}{idValue}

			i := 2
			for field := range allFields {
				columns = append(columns, field)
				placeholders = append(placeholders, fmt.Sprintf("$%p", i))

				if val, ok := data[field]; ok {
					values = append(values, fmt.Sprintf("%v", val))
				} else {
					values = append(values, nil)
				}
				i++
			}

			insertSQL := fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s)",
				tempTableName,
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "),
			)

			_, err = tx.ExecContext(ctx, insertSQL, values...)
			if err != nil {
				return err
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
				whereClause, whereArgs := buildWhereClause(filter)
				if whereClause != "" {
					updateSQL += " AND " + whereClause

					// Execute the update with the filter
					_, err = tx.ExecContext(ctx, updateSQL, whereArgs...)
					if err != nil {
						return err
					}
				}
			} else {
				// Execute the update without additional filters
				_, err = tx.ExecContext(ctx, updateSQL)
				if err != nil {
					return err
				}
			}

			// Drop the temporary table
			_, err = tx.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tempTableName))
			if err != nil {
				return err
			}
		}
	} else {
		// No common filter provided, update each object individually based on its ID
		for _, obj := range objects {
			// Convert object to map
			data, err := objectToMap(obj)
			if err != nil {
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

			// Build the UPDATE statement
			setClauses := make([]string, 0, len(data))
			values := make([]interface{}, 0, len(data)+1) // +1 for the ID in WHERE clause

			i := 1
			for k, v := range data {
				setClauses = append(setClauses, fmt.Sprintf("%s = $%p", k, i))
				values = append(values, v)
				i++
			}

			query := fmt.Sprintf(
				"UPDATE %s SET %s WHERE id = $%p",
				tableName,
				strings.Join(setClauses, ", "),
				i,
			)

			// Get the object ID for the WHERE clause
			id := obj.GetObjectID()
			if id == "" {
				continue // Skip objects without ID
			}

			values = append(values, id.Hex())

			// Execute the UPDATE statement
			_, err = tx.ExecContext(ctx, query, values...)
			if err != nil {
				return err
			}
		}
	}
	// Commit the transaction
	return tx.Commit()
}

func (p *driver) UpdateAll(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Check if update is empty
	if len(update) == 0 {
		return nil // Nothing to update
	}

	// Build the SQL UPDATE statement parts
	setClauses := make([]string, 0)
	values := make([]interface{}, 0)
	paramIndex := 1

	// Process MongoDB update operators
	for operator, fields := range update {
		switch operator {
		case "$set":
			// $set operator: directly set field values
			if setMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range setMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, paramIndex))
					values = append(values, value)
					paramIndex++
				}
			}

		case "$inc":
			// $inc operator: increment field values
			if incMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range incMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = %s + $%d", field, field, paramIndex))
					values = append(values, value)
					paramIndex++
				}
			}

		case "$mul":
			// $mul operator: multiply field values
			if mulMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range mulMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = %s * $%d", field, field, paramIndex))
					values = append(values, value)
					paramIndex++
				}
			}

		case "$unset":
			// $unset operator: set fields to NULL
			if unsetMap, ok := fields.(map[string]interface{}); ok {
				for field := range unsetMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = NULL", field))
				}
			}

		case "$min":
			// $min operator: update field if new value is less than current value
			if minMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range minMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = CASE WHEN $%d < %s THEN $%d ELSE %s END",
						field, paramIndex, field, paramIndex, field))
					values = append(values, value)
					paramIndex++
				}
			}

		case "$max":
			// $max operator: update field if new value is greater than current value
			if maxMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range maxMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = CASE WHEN $%d > %s THEN $%d ELSE %s END",
						field, paramIndex, field, paramIndex, field))
					values = append(values, value)
					paramIndex++
				}
			}

		case "$currentDate":
			// $currentDate operator: set fields to current date/time
			if dateMap, ok := fields.(map[string]interface{}); ok {
				for field := range dateMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = CURRENT_TIMESTAMP", field))
				}
			}

		default:
			// If not an operator, treat as a direct field update
			if !strings.HasPrefix(operator, "$") {
				setClauses = append(setClauses, fmt.Sprintf("%s = $%d", operator, paramIndex))
				values = append(values, fields)
				paramIndex++
			}
		}
	}
	// If no SET clauses were generated, there's nothing to update
	if len(setClauses) == 0 {
		return nil
	}

	// Build the complete UPDATE statement
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET %s",
		tableName,
		strings.Join(setClauses, ", "),
	)

	// Add WHERE clause based on the query filter
	if len(query) > 0 {
		whereClause, whereArgs := buildWhereClause(query)
		if whereClause != "" {
			updateSQL += " WHERE " + whereClause

			// Add the WHERE clause parameters to the values slice
			values = append(values, whereArgs...)
		}
	}

	// Execute the UPDATE statement
	result, err := p.db.ExecContext(ctx, updateSQL, values...)
	if err != nil {
		return err
	}

	// Optionally check affected rows
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}

	return nil
}

func (p *driver) Drop(ctx context.Context, object model.DBObject) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := object.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Validate table name to prevent SQL injection
	validTableName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(tableName)
	if !validTableName {
		return fmt.Errorf("invalid table name: %s", tableName)
	}

	// Build the SQL DROP TABLE statement WITH CASCADE
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
	// Execute the statement
	_, err := p.db.ExecContext(ctx, query)

	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	return nil
}

func (p *driver) CreateIndex(ctx context.Context, row model.DBObject, index model.Index) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Validate index
	if len(index.Keys) == 0 {
		return errors.New(types.ErrorIndexEmpty)
	}

	// Check if the index already exists
	exists, err := p.indexExists(ctx, tableName, index.Name)
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	if exists {
		// Index already exists, check if it has the same definition
		return errors.New(types.ErrorIndexAlreadyExist)
	}

	if index.IsTTLIndex {
		return errors.New(types.ErrorIndexTTLNotSupported)
	}

	var indexType string
	var indexFields []string

	// Process each key in the index
	for _, key := range index.Keys {
		for field, direction := range key {
			// In MongoDB, 1 means ascending, -1 means descending
			// In PostgreSQL, we use ASC or DESC
			var order string
			if direction == 1 {
				order = "ASC"
			} else if direction == -1 {
				order = "DESC"
			} else {
				// Handle special index types
				if field == "$text" {
					// MongoDB text index - PostgreSQL equivalent is GIN index with tsvector
					indexType = "GIN"
					// The actual field is in the nested map
					if textFields, ok := direction.(map[string]interface{}); ok {
						for textField := range textFields {
							indexFields = append(indexFields, fmt.Sprintf("to_tsvector('english', %s)", textField))
						}
					}
					continue
				} else {
					order = "ASC"
				}
			}
			indexFields = append(indexFields, fmt.Sprintf("%s %s", field, order))
		}
	}
	// If no index type was specified, use the default (BTREE)
	if indexType == "" {
		indexType = "BTREE"
	}

	// Build the CREATE INDEX statement
	createIndexSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s USING %s (%s)",
		index.Name,
		tableName,
		indexType,
		strings.Join(indexFields, ", "),
	)

	// Add CONCURRENTLY if background is true
	if index.Background {
		createIndexSQL = strings.Replace(createIndexSQL, "CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1)
	}

	// Execute the statement
	_, err = p.db.ExecContext(ctx, createIndexSQL)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

func (p *driver) GetIndexes(ctx context.Context, row model.DBObject) ([]model.Index, error) {
	// Check if the database connection is valid
	if p.db == nil {
		return nil, errors.New(types.ErrorSessionClosed)
	}

	tableName := row.TableName()
	if tableName == "" {
		return nil, errors.New("empty table name")
	}

	// Query to get index information from PostgreSQL system catalogs
	query := `
        SELECT
            i.relname AS index_name,
            a.attname AS column_name,
            ix.indisunique AS is_unique,
            am.amname AS index_type,
            CASE 
                WHEN ix.indoption[a.attnum-1] & 1 = 1 THEN -1  -- DESC order
                ELSE 1  -- ASC order
            END AS direction,
            obj_description(i.oid, 'pg_class') AS comment
        FROM
            pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_am am ON am.oid = i.relam
        WHERE
            t.relname = $1
            AND NOT ix.indisprimary  -- Exclude primary keys
        ORDER BY
            i.relname, a.attnum
    `

	rows, err := p.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	// Map to store indexes by name to group columns
	indexMap := make(map[string]*model.Index)

	// Process each row
	for rows.Next() {
		var indexName, columnName, indexType, comment string
		var isUnique bool
		var direction int

		err := rows.Scan(&indexName, &columnName, &isUnique, &indexType, &direction, &comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		// Get or create the index in the map
		idx, exists := indexMap[indexName]
		if !exists {
			idx = &model.Index{
				Name:       indexName,
				Background: false, // PostgreSQL doesn't store this information
				Keys:       []model.DBM{},
				IsTTLIndex: false,
				TTL:        0,
			}
			indexMap[indexName] = idx
		}
		// Add the column to the index keys
		columnDBM := model.DBM{
			columnName: direction,
		}
		idx.Keys = append(idx.Keys, columnDBM)
		idx.IsTTLIndex = false //we do not support ttl indexes for pg
	}

	// Check for any errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	// Convert the map to a slice
	indexes := make([]model.Index, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

func (p *driver) Ping(ctx context.Context) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Use the database/sql PingContext method to check if the database is reachable
	err := p.db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

func (p *driver) HasTable(ctx context.Context, tableName string) (bool, error) {
	// Check if the database connection is valid
	if p.db == nil {
		return false, errors.New(types.ErrorSessionClosed)
	}

	// Get the current schema (usually "public" by default)
	var schema string
	err := p.db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema)
	if err != nil {
		return false, fmt.Errorf("failed to get current schema: %w", err)
	}

	// Query to check if the table exists in the current schema
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = $1 
            AND table_name = $2
        )
    `

	var exists bool
	err = p.db.QueryRowContext(ctx, query, schema, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}

	return exists, nil
}

func (p *driver) DropDatabase(ctx context.Context) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the current database name
	if p.dbName == "" {
		// Query the current database name if not already stored
		var dbName string
		err := p.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("failed to get current database name: %w", err)
		}
		p.dbName = dbName
	}

	dbNameToDelete := p.dbName

	// In PostgreSQL, we cannot drop the currently connected database.
	// We need to provide instructions on how to drop the database manually.

	// Close the current connection
	err := p.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	// Set the driver's db to nil to prevent further use
	p.db = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

func (p *driver) Migrate(ctx context.Context, objects []model.DBObject, options ...model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Check if we have objects to migrate
	if len(objects) == 0 {
		return errors.New(types.ErrorEmptyRow)
	}

	// Check if we have multiple options
	if len(options) > 1 {
		return errors.New(types.ErrorRowOptDiffLenght)
	}

	// Start a transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Process each object
	for _, obj := range objects {
		// Get the table name
		tableName := obj.TableName()
		if tableName == "" {
			return errors.New(types.ErrorEmptyTableName)
		}

		// Check if the table already exists
		var exists bool
		existsQuery := `
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = current_schema() 
                AND table_name = $1
            )
        `

		err = tx.QueryRowContext(ctx, existsQuery, tableName).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if table exists: %w", err)
		}

		// If the table already exists, skip it
		if exists {
			continue
		}

		// Get the object's structure using reflection
		objType := reflect.TypeOf(obj)
		if objType.Kind() == reflect.Ptr {
			objType = objType.Elem()
		}

		// If it's not a struct, we can't create a table
		if objType.Kind() != reflect.Struct {
			return fmt.Errorf("object %s is not a struct", tableName)
		}

		// Build the CREATE TABLE statement
		columns := []string{
			"id TEXT PRIMARY KEY", // Assuming all objects have an ID field
		}

		// Process each field in the struct
		for i := 0; i < objType.NumField(); i++ {
			field := objType.Field(i)

			// Skip unexported fields
			if field.PkgPath != "" {
				continue
			}

			// Get the field name
			fieldName := field.Name
			// Check for json tag
			if jsonTag := field.Tag.Get("json"); jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" && parts[0] != "-" {
					fieldName = parts[0]
				}
			}

			// Skip the ID field as we've already added it
			if strings.ToLower(fieldName) == "id" || fieldName == "_id" {
				continue
			}

			// Map Go types to PostgreSQL types
			var pgType string
			switch field.Type.Kind() {
			case reflect.Bool:
				pgType = "BOOLEAN"
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				pgType = "BIGINT"
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				pgType = "BIGINT"
			case reflect.Float32, reflect.Float64:
				pgType = "DOUBLE PRECISION"
			case reflect.String:
				pgType = "TEXT"
			case reflect.Struct:
				// Check for time.Time
				if field.Type == reflect.TypeOf(time.Time{}) {
					pgType = "TIMESTAMP WITH TIME ZONE"
				} else {
					// For other structs, store as JSON
					pgType = "JSONB"
				}
			case reflect.Map, reflect.Slice, reflect.Array:
				// Store complex types as JSON
				pgType = "JSONB"
			default:
				// For other types, store as TEXT
				pgType = "TEXT"
			}

			// Add the column definition
			columns = append(columns, fmt.Sprintf("%s %s", fieldName, pgType))
		}

		// Create the table
		createTableSQL := fmt.Sprintf(
			"CREATE TABLE %s (%s)",
			tableName,
			strings.Join(columns, ", "),
		)

		_, err = tx.ExecContext(ctx, createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Apply any additional options
		if len(options) == 1 {
			opts := options[0]
			// Check for indexes to create
			if indexes, ok := opts["indexes"].([]model.Index); ok {
				for _, index := range indexes {
					// Create each index
					indexFields := []string{}
					for _, key := range index.Keys {
						for field, direction := range key {
							var order string
							if direction == 1 {
								order = "ASC"
							} else if direction == -1 {
								order = "DESC"
							} else {
								order = "ASC" // Default
							}
							indexFields = append(indexFields, fmt.Sprintf("%s %s", field, order))
						}
					}
					if len(indexFields) > 0 {
						createIndexSQL := fmt.Sprintf(
							"CREATE INDEX %s ON %s (%s)",
							index.Name,
							tableName,
							strings.Join(indexFields, ", "),
						)
						_, err = tx.ExecContext(ctx, createIndexSQL)
						if err != nil {
							return fmt.Errorf("failed to create index %s on table %s: %w", index.Name, tableName, err)
						}
					}
				}
			}
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *driver) DBTableStats(ctx context.Context, row model.DBObject) (model.DBM, error) {
	// Check if the database connection is valid
	if p.db == nil {
		return nil, errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return nil, errors.New(types.ErrorEmptyTableName)
	}

	// Initialize the result map
	stats := model.DBM{}

	// Get basic table statistics
	basicStatsQuery := `
        SELECT
            pg_stat_user_tables.n_live_tup AS row_count,
            pg_stat_user_tables.n_dead_tup AS dead_row_count,
            pg_stat_user_tables.n_mod_since_analyze AS modified_count,
            pg_stat_user_tables.last_vacuum AS last_vacuum,
            pg_stat_user_tables.last_autovacuum AS last_autovacuum,
            pg_stat_user_tables.last_analyze AS last_analyze,
            pg_stat_user_tables.last_autoanalyze AS last_autoanalyze,
            pg_stat_user_tables.vacuum_count AS vacuum_count,
            pg_stat_user_tables.autovacuum_count AS autovacuum_count,
            pg_stat_user_tables.analyze_count AS analyze_count,
            pg_stat_user_tables.autoanalyze_count AS autoanalyze_count,
            pg_class.reltuples AS estimated_row_count,
            pg_class.relpages AS page_count,
            pg_relation_size(pg_class.oid) AS size_bytes,
            pg_total_relation_size(pg_class.oid) AS total_size_bytes
        FROM
            pg_stat_user_tables
            JOIN pg_class ON pg_class.relname = pg_stat_user_tables.relname
        WHERE
            pg_stat_user_tables.relname = $1
    `

	var rowCount, deadRowCount, modifiedCount, vacuumCount, autovacuumCount, analyzeCount, autoanalyzeCount int64
	var estimatedRowCount, pageCount float64
	var sizeBytes, totalSizeBytes int64
	var lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze sql.NullTime

	err := p.db.QueryRowContext(ctx, basicStatsQuery, tableName).Scan(
		&rowCount,
		&deadRowCount,
		&modifiedCount,
		&lastVacuum,
		&lastAutovacuum,
		&lastAnalyze,
		&lastAutoanalyze,
		&vacuumCount,
		&autovacuumCount,
		&analyzeCount,
		&autoanalyzeCount,
		&estimatedRowCount,
		&pageCount,
		&sizeBytes,
		&totalSizeBytes,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("table %s not found", tableName)
		}
		return nil, fmt.Errorf("failed to get table statistics: %w", err)
	}

	// Add basic statistics to the result map
	stats["row_count"] = rowCount
	stats["dead_row_count"] = deadRowCount
	stats["modified_count"] = modifiedCount
	stats["vacuum_count"] = vacuumCount
	stats["autovacuum_count"] = autovacuumCount
	stats["analyze_count"] = analyzeCount
	stats["autoanalyze_count"] = autoanalyzeCount
	stats["estimated_row_count"] = estimatedRowCount
	stats["page_count"] = pageCount
	stats["size_bytes"] = sizeBytes
	stats["total_size_bytes"] = totalSizeBytes

	// Handle nullable timestamps
	if lastVacuum.Valid {
		stats["last_vacuum"] = lastVacuum.Time
	} else {
		stats["last_vacuum"] = nil
	}

	if lastAutovacuum.Valid {
		stats["last_autovacuum"] = lastAutovacuum.Time
	} else {
		stats["last_autovacuum"] = nil
	}

	if lastAnalyze.Valid {
		stats["last_analyze"] = lastAnalyze.Time
	} else {
		stats["last_analyze"] = nil
	}

	if lastAutoanalyze.Valid {
		stats["last_autoanalyze"] = lastAutoanalyze.Time
	} else {
		stats["last_autoanalyze"] = nil
	}

	// Get index statistics
	indexStatsQuery := `
        SELECT
            indexrelname AS index_name,
            idx_scan AS scan_count,
            idx_tup_read AS tuples_read,
            idx_tup_fetch AS tuples_fetched,
            pg_relation_size(indexrelid) AS index_size_bytes
        FROM
            pg_stat_user_indexes
        WHERE
            relname = $1
    `

	indexRows, err := p.db.QueryContext(ctx, indexStatsQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get index statistics: %w", err)
	}
	defer indexRows.Close()

	// Initialize indexes array
	indexes := []model.DBM{}

	// Process each index
	for indexRows.Next() {
		var indexName string
		var scanCount, tuplesRead, tuplesFetched, indexSizeBytes int64

		err := indexRows.Scan(&indexName, &scanCount, &tuplesRead, &tuplesFetched, &indexSizeBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		indexStats := model.DBM{
			"name":           indexName,
			"scan_count":     scanCount,
			"tuples_read":    tuplesRead,
			"tuples_fetched": tuplesFetched,
			"size_bytes":     indexSizeBytes,
		}

		indexes = append(indexes, indexStats)
	}

	// Check for errors during iteration
	if err = indexRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	// Add indexes to the result map
	stats["indexes"] = indexes

	// Get column statistics
	columnStatsQuery := `
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            s.null_frac AS null_fraction,
            s.avg_width AS average_width,
            s.n_distinct AS distinct_values,
            s.most_common_vals AS most_common_values,
            s.most_common_freqs AS most_common_frequencies,
            s.histogram_bounds AS histogram_bounds,
            s.correlation AS correlation
        FROM
            pg_catalog.pg_attribute a
            LEFT JOIN pg_catalog.pg_stats s ON (s.tablename = $1 AND s.attname = a.attname)
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        WHERE
            c.relname = $1
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY
            a.attnum
    `

	columnRows, err := p.db.QueryContext(ctx, columnStatsQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get column statistics: %w", err)
	}
	defer columnRows.Close()

	// Initialize columns array
	columns := []model.DBM{}

	// Process each column
	for columnRows.Next() {
		var columnName, dataType string
		var nullFraction, averageWidth float64
		var distinctValues float64
		var mostCommonValues, mostCommonFreqs, histogramBounds, correlation sql.NullString

		err := columnRows.Scan(
			&columnName,
			&dataType,
			&nullFraction,
			&averageWidth,
			&distinctValues,
			&mostCommonValues,
			&mostCommonFreqs,
			&histogramBounds,
			&correlation,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}

		columnStats := model.DBM{
			"name":            columnName,
			"data_type":       dataType,
			"null_fraction":   nullFraction,
			"average_width":   averageWidth,
			"distinct_values": distinctValues,
		}

		// Handle nullable strings
		if mostCommonValues.Valid {
			columnStats["most_common_values"] = mostCommonValues.String
		}

		if mostCommonFreqs.Valid {
			columnStats["most_common_frequencies"] = mostCommonFreqs.String
		}

		if histogramBounds.Valid {
			columnStats["histogram_bounds"] = histogramBounds.String
		}

		if correlation.Valid {
			columnStats["correlation"] = correlation.String
		}

		columns = append(columns, columnStats)
	}

	// Check for errors during iteration
	if err = columnRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column rows: %w", err)
	}

	// Add columns to the result map
	stats["columns"] = columns

	// Add MongoDB-compatible fields
	stats["ns"] = tableName               // Namespace (table name in PostgreSQL)
	stats["count"] = rowCount             // Document count
	stats["size"] = sizeBytes             // Size in bytes
	stats["storageSize"] = totalSizeBytes // Total storage size
	stats["capped"] = false               // PostgreSQL doesn't have capped collections
	stats["wiredTiger"] = nil             // PostgreSQL doesn't use WiredTiger

	return stats, nil
}

func (p *driver) Aggregate(ctx context.Context, row model.DBObject, pipeline []model.DBM) ([]model.DBM, error) {
	if p.db == nil {
		return []model.DBM{}, errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return nil, errors.New(types.ErrorEmptyTableName)
	}

	// Check if pipeline is empty
	if len(pipeline) == 0 {
		return nil, errors.New("empty aggregation pipeline")
	}

	// Translate MongoDB aggregation pipeline to SQL
	sqlQuery, args, err := translateAggregationPipeline(tableName, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to translate aggregation pipeline: %w", err)
	}

	// Execute the query
	rows, err := p.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute aggregation query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Process the results
	results := []model.DBM{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		err := rows.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a DBM for the row
		rowMap := model.DBM{}
		// Set values in the map
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			rowMap[col] = val
		}

		results = append(results, rowMap)
	}
	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

func (p *driver) CleanIndexes(ctx context.Context, row model.DBObject) error {
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()
	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Query to get all indexes for the table, excluding primary key and unique constraints
	query := `
        SELECT
            i.relname AS index_name
        FROM
            pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            LEFT JOIN pg_constraint c ON c.conindid = ix.indexrelid
        WHERE
            t.relname = $1
            AND c.contype IS NULL  -- Exclude indexes that are part of constraints
            AND NOT ix.indisprimary  -- Exclude primary key indexes
        ORDER BY
            i.relname
    `

	// Execute the query
	rows, err := p.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	// Start a transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Drop each index
	indexCount := 0
	for rows.Next() {
		var indexName string

		err := rows.Scan(&indexName)
		if err != nil {
			return fmt.Errorf("failed to scan index name: %w", err)
		}

		// Build the DROP INDEX statement
		dropIndexSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName)

		// Execute the statement
		_, err = tx.ExecContext(ctx, dropIndexSQL)
		if err != nil {
			return fmt.Errorf("failed to drop index %s: %w", indexName, err)
		}

		indexCount++
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating index rows: %w", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *driver) Upsert(ctx context.Context, row model.DBObject, query, update model.DBM) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Get the table name from the DBObject
	tableName := row.TableName()

	if tableName == "" {
		return errors.New(types.ErrorEmptyTableName)
	}

	// Start a transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// First, try to update the existing record
	updateSQL, updateArgs, err := buildUpdateSQL(tableName, query, update)
	if err != nil {
		return fmt.Errorf("failed to build update SQL: %w", err)
	}

	result, err := tx.ExecContext(ctx, updateSQL, updateArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// If no rows were affected, perform an insert
	if rowsAffected == 0 {
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

		// Build the INSERT statement
		insertSQL, insertArgs, err := buildInsertSQL(tableName, insertData)
		if err != nil {
			return fmt.Errorf("failed to build insert SQL: %w", err)
		}

		// Add RETURNING clause to get the inserted row
		insertSQL += " RETURNING *"

		// Execute the insert
		resultsRows, err := tx.QueryContext(ctx, insertSQL, insertArgs...)
		if err != nil {
			return fmt.Errorf("failed to execute insert: %w", err)
		}
		defer resultsRows.Close()

		columns, err := resultsRows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Scan the result into the row object
		if resultsRows.Next() {
			err = scanRowToObject(resultsRows, columns, row)
			if err != nil {
				return fmt.Errorf("failed to scan inserted row: %w", err)
			}
		} else {
			return errors.New("no rows returned from insert")
		}
	} else {
		// Update succeeded, get the updated row
		whereClause, whereArgs := buildWhereClause(query)
		selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s", tableName, whereClause)

		// Execute the select
		resultRows, err := tx.QueryContext(ctx, selectSQL, whereArgs...)
		if err != nil {
			return fmt.Errorf("failed to execute select: %w", err)
		}
		defer resultRows.Close()

		// Get column names
		columns, err := resultRows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Scan the result into the row object
		if resultRows.Next() {
			err = scanRowToObject(resultRows, columns, row)
			if err != nil {
				return fmt.Errorf("failed to scan updated row: %w", err)
			}
		} else {
			return sql.ErrNoRows
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *driver) GetDatabaseInfo(ctx context.Context) (utils.Info, error) {
	if p.db == nil {
		return utils.Info{}, errors.New(types.ErrorSessionClosed)
	}

	// Initialize the result structure
	info := utils.Info{
		Type: utils.PostgresDB, // Assuming utils.PostgreSQL is defined in the utils package
	}
	// Get basic database information
	basicInfoQuery := `
        SELECT
            current_database() AS db_name,
            current_user AS db_user,
            version() AS version,
            current_setting('server_version') AS server_version,
            pg_database_size(current_database()) AS db_size_bytes,
            pg_postmaster_start_time() AS start_time,
            current_setting('max_connections') AS max_connections
    `
	var dbName, dbUser, version, serverVersion string
	var dbSizeBytes int64
	var startTime time.Time
	var maxConnections string

	err := p.db.QueryRowContext(ctx, basicInfoQuery).Scan(
		&dbName,
		&dbUser,
		&version,
		&serverVersion,
		&dbSizeBytes,
		&startTime,
		&maxConnections,
	)

	if err != nil {
		return utils.Info{}, fmt.Errorf("failed to get database information: %w", err)
	}

	// Populate the Info structure
	info.Name = dbName
	info.User = dbUser
	info.Version = serverVersion
	info.FullVersion = version
	info.SizeBytes = dbSizeBytes
	info.StartTime = startTime

	// Convert max_connections to int
	maxConn, err := strconv.Atoi(maxConnections)
	if err == nil {
		info.MaxConnections = maxConn
	}

	// Get current connection count
	connectionCountQuery := `
        SELECT
            count(*) AS connection_count
        FROM
            pg_stat_activity
        WHERE
            datname = current_database()
    `

	var connectionCount int

	err = p.db.QueryRowContext(ctx, connectionCountQuery).Scan(&connectionCount)
	if err == nil {
		info.CurrentConnections = connectionCount
	}

	// Get table count
	tableCountQuery := `
        SELECT
            count(*) AS table_count
        FROM
            information_schema.tables
        WHERE
            table_schema = current_schema()
            AND table_type = 'BASE TABLE'
    `

	var tableCount int

	err = p.db.QueryRowContext(ctx, tableCountQuery).Scan(&tableCount)
	if err == nil {
		info.TableCount = tableCount
	}

	return info, nil
}

func (p *driver) GetTables(ctx context.Context) ([]string, error) {
	if p.db == nil {
		return []string{}, errors.New(types.ErrorSessionClosed)
	}

	// Query to get all tables in the current schema
	query := `
        SELECT 
            table_name 
        FROM 
            information_schema.tables 
        WHERE 
            table_schema = current_schema()
            AND table_type = 'BASE TABLE'
        ORDER BY 
            table_name
    `

	// Execute the query
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	// Initialize the result slice
	tables := []string{}

	// Process each row
	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}

		tables = append(tables, tableName)
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table rows: %w", err)
	}

	return tables, nil
}

func (p *driver) DropTable(ctx context.Context, name string) (int, error) {
	if p.db == nil {
		return 0, errors.New(types.ErrorSessionClosed)
	}

	// Validate table name
	if name == "" {
		return 0, errors.New(types.ErrorEmptyTableName)
	}

	// Check if the table exists before dropping it
	existsQuery := `
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = current_schema() 
            AND table_name = $1
        )
    `

	var exists bool
	err := p.db.QueryRowContext(ctx, existsQuery, name).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		// Table doesn't exist, return 0 affected rows
		return 0, nil
	}

	// Get the number of rows in the table before dropping it
	// This is to return the number of affected rows
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", name)

	var rowCount int
	err = p.db.QueryRowContext(ctx, countQuery).Scan(&rowCount)
	if err != nil {
		rowCount = 0
	}

	// Build the SQL DROP TABLE statement
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)

	// Execute the statement
	_, err = p.db.ExecContext(ctx, dropQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to drop table %s: %w", name, err)
	}

	// Return the number of rows that were in the table
	return rowCount, nil
}

func (p *driver) Connect(opts *types.ClientOpts) error {
	connStr := opts.ConnectionString
	if opts.UseSSL {
		connStr += " sslmode=require"
		if opts.SSLInsecureSkipVerify {
			connStr += " sslmode=require sslrootcert=verify-ca"
		}
		if opts.SSLCAFile != "" {
			connStr += fmt.Sprintf(" sslrootcert=%s", opts.SSLCAFile)
		}
		if opts.SSLPEMKeyfile != "" {
			connStr += fmt.Sprintf(" sslcert=%s", opts.SSLPEMKeyfile)
		}
	} else {
		connStr += " sslmode=disable"
	}

	var err error
	p.db, err = sql.Open("driver", connStr)
	if err != nil {
		return err
	}

	// Set connection timeout
	if opts.ConnectionTimeout > 0 {
		p.db.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	// Extract database name from connection string
	parts := strings.Split(connStr, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "dbname=") {
			p.dbName = strings.TrimPrefix(part, "dbname=")
			break
		}
	}

	return p.db.Ping()
}

// Helper function to check if an index exists
func (p *driver) indexExists(ctx context.Context, tableName, indexName string) (bool, error) {
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM pg_indexes 
            WHERE tablename = $1 
            AND indexname = $2
        )
    `

	var exists bool
	err := p.db.QueryRowContext(ctx, query, tableName, indexName).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func NewPostgresDriver(opts *types.ClientOpts) (*driver, error) {
	driver := &driver{}
	err := driver.Connect(opts)
	if err != nil {
		return nil, err
	}
	return driver, nil
}

// Helper functions
func objectToMap(obj interface{}) (map[string]interface{}, error) {
	// Convert object to JSON and then to map
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	return result, err
}

func buildWhereClause(filter model.DBM) (string, []interface{}) {
	if len(filter) == 0 {
		return "", nil
	}

	conditions := make([]string, 0, len(filter))
	values := make([]interface{}, 0, len(filter))

	i := 1
	for k, v := range filter {

		// Handle top-level MongoDB logica
		if strings.HasPrefix(k, "$") {
			switch k {
			case "$or":
				if orConditions, ok := v.([]model.DBM); ok {
					orClauses := make([]string, 0, len(orConditions))
					for _, orCond := range orConditions {
						whereClause, whereArgs := buildWhereClause(orCond)
						if whereClause != "" {
							orClauses = append(orClauses, "("+whereClause+")")
							values = append(values, whereArgs...)
							i += len(whereArgs)
						}
					}
					if len(orClauses) > 0 {
						conditions = append(conditions, "("+strings.Join(orClauses, " OR ")+")")
					}
				}
			case "$and":
				// Handle $and operator
				if andConditions, ok := v.([]model.DBM); ok {
					andClauses := make([]string, 0, len(andConditions))
					for _, andCond := range andConditions {
						whereClause, whereArgs := buildWhereClause(andCond)
						if whereClause != "" {
							andClauses = append(andClauses, "("+whereClause+")")
							values = append(values, whereArgs...)
							i += len(whereArgs)
						}
					}
					if len(andClauses) > 0 {
						conditions = append(conditions, "("+strings.Join(andClauses, " AND ")+")")
					}
				}
				// Add more top-level operators as needed
			}
			continue
		}
		// Handle different value types
		switch val := v.(type) {
		case nil:
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", k))
		case model.ObjectID:
			conditions = append(conditions, fmt.Sprintf("%s = $%d", k, i))
			values = append(values, val.Hex())
			i++
		case map[string]interface{}:
			// This would handle MongoDB query operators like {field: {$gt: value}}
			// For simplicity, we're using basic equality in this example
			for op, opVal := range val {
				switch op {
				case "$eq":
					conditions = append(conditions, fmt.Sprintf("%s = $%d", k, i))
					values = append(values, opVal)
					i++
				case "$gt":
					conditions = append(conditions, fmt.Sprintf("%s > $%d", k, i))
					values = append(values, opVal)
					i++
				case "$lt":
					conditions = append(conditions, fmt.Sprintf("%s < $%d", k, i))
					values = append(values, opVal)
					i++
				case "$gte":
					conditions = append(conditions, fmt.Sprintf("%s >= $%d", k, i))
					values = append(values, opVal)
					i++
				case "$lte":
					conditions = append(conditions, fmt.Sprintf("%s <= $%d", k, i))
					values = append(values, opVal)
					i++
				case "$ne":
					conditions = append(conditions, fmt.Sprintf("%s != $%d", k, i))
					values = append(values, opVal)
					i++
				case "$in":
					// Handle IN operator
					if arr, ok := opVal.([]interface{}); ok {
						placeholders := make([]string, len(arr))
						for j := range arr {
							placeholders[j] = fmt.Sprintf("$%d", i)
							values = append(values, arr[j])
							i++
						}
						conditions = append(conditions, fmt.Sprintf("%s IN (%s)", k, strings.Join(placeholders, ", ")))
					}
				// Add more MongoDB operators as needed
				case "$nin":
					// Handle NOT IN operator
					if arr, ok := opVal.([]interface{}); ok {
						placeholders := make([]string, len(arr))
						for j := range arr {
							placeholders[j] = fmt.Sprintf("$%d", i)
							values = append(values, arr[j])
							i++
						}
						conditions = append(conditions, fmt.Sprintf("%s NOT IN (%s)", k, strings.Join(placeholders, ", ")))
					}
				case "$regex":
					// Handle regex pattern matching
					regexStr, ok := opVal.(string)
					if ok {
						conditions = append(conditions, fmt.Sprintf("%s ~ $%d", k, i))
						values = append(values, regexStr)
						i++
					}
				}
			}
		default:
			conditions = append(conditions, fmt.Sprintf("%s = $%d", k, i))
			values = append(values, val)
			i++
		}
		conditions = append(conditions, fmt.Sprintf("%s = $%d", k, i))
		values = append(values, v)
		i++
	}
	return strings.Join(conditions, " AND "), values
}

func scanRowToObject(rows *sql.Rows, columns []string, dest interface{}) error {
	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	// Scan the row into the values
	err := rows.Scan(values...)
	if err != nil {
		return err
	}

	// Create a map of column name to value
	rowMap := make(map[string]interface{})
	for i, col := range columns {
		val := *(values[i].(*interface{}))
		rowMap[col] = val
	}

	// Convert map to JSON and then to object
	jsonData, err := json.Marshal(rowMap)
	if err != nil {
		return err
	}

	return json.Unmarshal(jsonData, dest)
}

// Helper function to build an UPDATE SQL statement
func buildUpdateSQL(tableName string, query, update model.DBM) (string, []interface{}, error) {
	if len(query) == 0 {
		return "", nil, errors.New(types.ErrorEmptyQuery)
	}

	// Build the SET clause
	setClauses := []string{}
	args := []interface{}{}
	argIndex := 1

	// Process MongoDB update operators
	for operator, fields := range update {
		switch operator {
		case "$set":
			// $set operator: directly set field values
			if setMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range setMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, argIndex))
					args = append(args, value)
					argIndex++
				}
			}

		case "$inc":
			// $inc operator: increment field values
			if incMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range incMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = %s + $%d", field, field, argIndex))
					args = append(args, value)
					argIndex++
				}
			}

		case "$mul":
			// $mul operator: multiply field values
			if mulMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range mulMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = %s * $%d", field, field, argIndex))
					args = append(args, value)
					argIndex++
				}
			}

		case "$unset":
			// $unset operator: set fields to NULL
			if unsetMap, ok := fields.(map[string]interface{}); ok {
				for field := range unsetMap {
					setClauses = append(setClauses, fmt.Sprintf("%s = NULL", field))
				}
			}

		default:
			// If not an operator, treat as a direct field update
			if !strings.HasPrefix(operator, "$") {
				setClauses = append(setClauses, fmt.Sprintf("%s = $%d", operator, argIndex))
				args = append(args, fields)
				argIndex++
			}
		}
	}

	// If no SET clauses were generated, return an error
	if len(setClauses) == 0 {
		return "", nil, errors.New("no fields to update")
	}

	// Build the WHERE clause
	whereClause, whereArgs := buildWhereClause(query)

	// Add WHERE args to the args slice
	for _, arg := range whereArgs {
		args = append(args, arg)
		argIndex++
	}

	// Build the complete UPDATE statement
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		whereClause,
	)

	return updateSQL, args, nil
}

// Helper function to build an INSERT SQL statement
func buildInsertSQL(tableName string, data model.DBM) (string, []interface{}, error) {
	if len(data) == 0 {
		return "", nil, errors.New("empty data")
	}

	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	i := 1
	for k, v := range data {
		columns = append(columns, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, v)
		i++
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return insertSQL, args, nil
}

func translateAggregationPipeline(tableName string, pipeline []model.DBM) (string, []interface{}, error) {
	// Initialize SQL parts
	var selectClause string = "*"
	var fromClause string = tableName
	var whereClause string
	var groupByClause string
	var havingClause string
	var orderByClause string
	var limitClause string
	var offsetClause string

	// Initialize args for parameterized query
	var args []interface{}
	var argIndex int = 1

	// Process each stage in the pipeline
	for _, stage := range pipeline {
		// Each stage should have exactly one key (the operator)
		if len(stage) != 1 {
			return "", nil, errors.New("each pipeline stage must have exactly one operator")
		}

		// Get the operator and its value
		var operator string
		var value interface{}
		for k, v := range stage {
			operator = k
			value = v
		}

		// Process the operator
		switch operator {
		case "$match":
			// $match: Filter documents (WHERE clause)
			if matchExpr, ok := value.(model.DBM); ok {
				matchWhere, matchArgs := buildWhereClause(matchExpr)
				if matchWhere != "" {
					if whereClause == "" {
						whereClause = matchWhere
					} else {
						whereClause = whereClause + " AND (" + matchWhere + ")"
					}
					args = append(args, matchArgs...)
					argIndex += len(matchArgs)
				}
			} else {
				return "", nil, errors.New("$match value must be a DBM")
			}
		case "$group":
			// $group: Group documents (GROUP BY clause)
			if groupExpr, ok := value.(model.DBM); ok {
				// Process _id field which defines the grouping keys
				if idExpr, ok := groupExpr["_id"]; ok {
					if idMap, ok := idExpr.(model.DBM); ok {
						// _id is a document with field:expression pairs
						groupFields := []string{}
						for _, expr := range idMap {
							// For simplicity, we assume the expression is just a field name
							if fieldName, ok := expr.(string); ok {
								if strings.HasPrefix(fieldName, "$") {
									// Remove the $ prefix for field references
									fieldName = strings.TrimPrefix(fieldName, "$")
								}
								groupFields = append(groupFields, fieldName)
							} else {
								return "", nil, errors.New("complex group expressions not supported")
							}
						}
						if len(groupFields) > 0 {
							groupByClause = strings.Join(groupFields, ", ")
						}
					} else if idExpr == nil {
						// _id: null means group all documents
						groupByClause = ""
					} else if fieldName, ok := idExpr.(string); ok {
						// _id is a simple field reference
						if strings.HasPrefix(fieldName, "$") {
							// Remove the $ prefix for field references
							fieldName = strings.TrimPrefix(fieldName, "$")
						}
						groupByClause = fieldName
					} else {
						return "", nil, errors.New("complex group expressions not supported")
					}
				}

				// Process aggregation functions
				selectParts := []string{}

				// Add group by fields to select clause
				if groupByClause != "" {
					groupFields := strings.Split(groupByClause, ", ")
					for _, field := range groupFields {
						selectParts = append(selectParts, field)
					}
				}

				// Add aggregation functions to select clause
				for field, expr := range groupExpr {
					if field == "_id" {
						continue // Already processed
					}

					if exprMap, ok := expr.(model.DBM); ok {
						for funcName, funcArg := range exprMap {
							// Map MongoDB aggregation functions to SQL
							var sqlFunc string
							switch funcName {
							case "$sum":
								sqlFunc = "SUM"
							case "$avg":
								sqlFunc = "AVG"
							case "$min":
								sqlFunc = "MIN"
							case "$max":
								sqlFunc = "MAX"
							case "$count":
								sqlFunc = "COUNT"
							default:
								return "", nil, fmt.Errorf("unsupported aggregation function: %s", funcName)
							}

							// For simplicity, we assume the function argument is just a field name or a literal
							var argStr string
							if funcArg == 1 && funcName == "$sum" {
								// Special case for $sum: 1 which is COUNT(*)
								argStr = "*"
							} else if fieldName, ok := funcArg.(string); ok {
								if strings.HasPrefix(fieldName, "$") {
									// Remove the $ prefix for field references
									fieldName = strings.TrimPrefix(fieldName, "$")
								}
								argStr = fieldName
							} else {
								// For literals, add as a parameter
								argStr = fmt.Sprintf("$%d", argIndex)
								args = append(args, funcArg)
								argIndex++
							}

							selectParts = append(selectParts, fmt.Sprintf("%s(%s) AS %s", sqlFunc, argStr, field))
						}
					} else {
						return "", nil, fmt.Errorf("invalid aggregation expression for field %s", field)
					}
				}

				if len(selectParts) > 0 {
					selectClause = strings.Join(selectParts, ", ")
				}
			} else {
				return "", nil, errors.New("$group value must be a DBM")
			}

		case "$project":
			// $project: Reshape documents (SELECT clause)
			if projectExpr, ok := value.(model.DBM); ok {
				projectParts := []string{}
				for field, include := range projectExpr {
					if include == 1 || include == true {
						// Include the field as is
						projectParts = append(projectParts, field)
					} else if include == 0 || include == false {
						// Exclude the field (do nothing)
					} else if exprMap, ok := include.(model.DBM); ok {
						// Field has an expression
						for exprOp, exprVal := range exprMap {
							switch exprOp {
							case "$concat":
								// Concatenate strings
								if concatArray, ok := exprVal.([]interface{}); ok {
									concatParts := []string{}
									for _, part := range concatArray {
										if partStr, ok := part.(string); ok {
											if strings.HasPrefix(partStr, "$") {
												// Field reference
												concatParts = append(concatParts, strings.TrimPrefix(partStr, "$"))
											} else {
												// String literal
												concatParts = append(concatParts, fmt.Sprintf("'%s'", partStr))
											}
										} else {
											return "", nil, errors.New("$concat arguments must be strings")
										}
									}
									projectParts = append(projectParts, fmt.Sprintf("CONCAT(%s) AS %s", strings.Join(concatParts, ", "), field))
								} else {
									return "", nil, errors.New("$concat value must be an array")
								}
							default:
								return "", nil, fmt.Errorf("unsupported projection operator: %s", exprOp)
							}
						}
					} else {
						return "", nil, fmt.Errorf("invalid projection expression for field %s", field)
					}
				}

				if len(projectParts) > 0 {
					selectClause = strings.Join(projectParts, ", ")
				}
			} else {
				return "", nil, errors.New("$project value must be a DBM")
			}

		case "$sort":
			// $sort: Sort documents (ORDER BY clause)
			if sortExpr, ok := value.(model.DBM); ok {
				sortParts := []string{}
				for field, direction := range sortExpr {
					var dirStr string
					if dir, ok := direction.(int); ok {
						if dir == 1 {
							dirStr = "ASC"
						} else if dir == -1 {
							dirStr = "DESC"
						} else {
							return "", nil, fmt.Errorf("invalid sort direction for field %s: %d", field, dir)
						}
						sortParts = append(sortParts, fmt.Sprintf("%s %s", field, dirStr))
					} else {
						return "", nil, fmt.Errorf("sort direction for field %s must be an integer", field)
					}
				}

				if len(sortParts) > 0 {
					orderByClause = strings.Join(sortParts, ", ")
				}
			} else {
				return "", nil, errors.New("$sort value must be a DBM")
			}

		case "$limit":
			// $limit: Limit the number of documents (LIMIT clause)
			if limit, ok := value.(int); ok {
				limitClause = fmt.Sprintf("%d", limit)
			} else {
				return "", nil, errors.New("$limit value must be an integer")
			}

		case "$skip":
			// $skip: Skip documents (OFFSET clause)
			if skip, ok := value.(int); ok {
				offsetClause = fmt.Sprintf("%d", skip)
			} else {
				return "", nil, errors.New("$skip value must be an integer")
			}

		default:
			return "", nil, fmt.Errorf("unsupported aggregation operator: %s", operator)
		}
	}
	// Build the SQL query
	query := fmt.Sprintf("SELECT %s FROM %s", selectClause, fromClause)

	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	if groupByClause != "" {
		query += fmt.Sprintf(" GROUP BY %s", groupByClause)
	}

	if havingClause != "" {
		query += fmt.Sprintf(" HAVING %s", havingClause)
	}

	if orderByClause != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderByClause)
	}

	if limitClause != "" {
		query += fmt.Sprintf(" LIMIT %s", limitClause)
	}

	if offsetClause != "" {
		query += fmt.Sprintf(" OFFSET %s", offsetClause)
	}

	return query, args, nil
}
