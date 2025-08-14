package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"strings"
)

type IndexRow struct {
	IndexName  string
	ColumnName string
	IsUnique   bool
	IndexType  string
	Direction  int
	Comment    *string // Using pointer for nullable string
}

func (d *driver) CreateIndex(ctx context.Context, row model.DBObject, index model.Index) error {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	// Validate index
	if len(index.Keys) == 0 {
		return errors.New(types.ErrorIndexEmpty)
	}

	// Check if the index already exists
	exists, err := d.indexExists(ctx, tableName, index.Name)
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

	indexType := "BTREE" // Default index type
	var indexFields []string

	// Process each key in the index
	for _, key := range index.Keys {
		for field, direction := range key {
			// In MongoDB, 1 means ascending, -1 means descending
			// In PostgresSQL, we use ASC or DESC
			var order string
			if direction == 1 {
				order = "ASC"
			} else if direction == -1 {
				order = "DESC"
			} else {
				// Handle special index types
				if field == "$text" {
					// MongoDB text index - PostgresSQL equivalent is GIN index with tsvector
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
	err = d.db.WithContext(ctx).Exec(createIndexSQL).Error
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

func (d *driver) GetIndexes(ctx context.Context, row model.DBObject) ([]model.Index, error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return nil, err
	}

	// Query to get index information from PostgresSQL system catalogs
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

	var rows []IndexRow
	err = d.db.WithContext(ctx).Raw(query, tableName).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}

	// Map to store indexes by name to group columns
	indexMap := make(map[string]*model.Index)

	// Process each row
	for _, row := range rows {
		// Get or create the index in the map
		idx, exists := indexMap[row.IndexName]
		if !exists {
			idx = &model.Index{
				Name:       row.IndexName,
				Background: false, // PostgreSQL doesn't store this information
				Keys:       []model.DBM{},
				IsTTLIndex: false,
				TTL:        0,
			}
			indexMap[row.IndexName] = idx
		}

		// Add the column to the index keys
		columnDBM := model.DBM{
			row.ColumnName: row.Direction,
		}
		idx.Keys = append(idx.Keys, columnDBM)
		idx.IsTTLIndex = false
	}

	// Convert the map to a slice
	indexes := make([]model.Index, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

func (d *driver) CleanIndexes(ctx context.Context, row model.DBObject) error {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
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

	var indexNames []string
	// Execute the query
	err = d.db.WithContext(ctx).Raw(query, tableName).Pluck("index_name", &indexNames).Error
	if err != nil {
		return fmt.Errorf("failed to query indexes: %w", err)
	}
	// If no indexes to drop, return early
	if len(indexNames) == 0 {
		return nil
	}

	// Start a transaction
	tx := d.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure the transaction is rolled back if there's an error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Drop each index
	for _, indexName := range indexNames {
		dropIndexSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName)
		if err := tx.Exec(dropIndexSQL).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to drop index %s: %w", indexName, err)
		}
	}

	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Helper function to check if an index exists
func (d *driver) indexExists(ctx context.Context, tableName, indexName string) (bool, error) {
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM pg_indexes 
            WHERE tablename = $1 
            AND indexname = $2
        )
    `

	var exists bool
	err := d.db.WithContext(ctx).Raw(query, tableName, indexName).Scan(&exists).Error
	if err != nil {
		return false, err
	}

	return exists, nil
}
