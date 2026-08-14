package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/lib/pq"

	"github.com/TykTechnologies/storage/persistent/model"
)

type IndexRow struct {
	IndexName  string
	ColumnName string
	IsUnique   bool
	IndexType  string
	Direction  int
	Comment    *string // Using pointer for nullable string
}

func sanitizeIdentifier(s string) (string, error) {
	if matched := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(s); !matched {
		return "", fmt.Errorf("invalid identifier: %s", s)
	}

	return pq.QuoteIdentifier(s), nil // use pq or pgx quoting
}

// CreateIndex creates a database index on the specified table for the given fields.
// Returns an error if index creation fails.
func (d *driver) CreateIndex(ctx context.Context, row model.DBObject, index model.Index) error {
	// Validate table
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	tableName, err = sanitizeIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Validate index
	if len(index.Keys) == 0 {
		return ErrorIndexEmpty
	}

	if index.IsTTLIndex && len(index.Keys) > 1 {
		return ErrorIndexComposedTTL
	}

	// Generate index name if not provided
	indexName := index.Name
	if indexName == "" {
		parts := []string{}

		for _, key := range index.Keys {
			for field, direction := range key {
				dirStr := getDirectionString(direction)
				parts = append(parts, field+"_"+dirStr)
			}
		}

		indexName = strings.Join(parts, "_")
	}

	indexName, err = sanitizeIdentifier(indexName)
	if err != nil {
		return fmt.Errorf("invalid index name: %w", err)
	}

	// Build column list safely
	var indexFields []string
	indexType := "BTREE"

	for _, key := range index.Keys {
		for field, direction := range key {
			if field == "_id" {
				field = "id"
			}

			col, err := sanitizeIdentifier(field)
			if err != nil {
				return fmt.Errorf("invalid column name: %w", err)
			}

			switch v := direction.(type) {
			case int, int32, int64:
				sortDir := "ASC"

				switch val := v.(type) {
				case int:
					if val < 0 {
						sortDir = "DESC"
					}
				case int32:
					if val < 0 {
						sortDir = "DESC"
					}
				case int64:
					if val < 0 {
						sortDir = "DESC"
					}
				}

				indexFields = append(indexFields, fmt.Sprintf("%s %s", col, sortDir))
			case string:
				if v == "2dsphere" {
					indexType = "GIST"

					indexFields = append(indexFields, col)
				} else {
					indexFields = append(indexFields, fmt.Sprintf("%s ASC", col))
				}
			default:
				indexFields = append(indexFields, fmt.Sprintf("%s ASC", col))
			}
		}
	}

	// Check if the index already exists
	exists, err := d.indexExists(ctx, tableName, indexName)
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}

	if exists {
		return ErrorIndexAlreadyExist
	}

	// Build CREATE INDEX statement
	createSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s USING %s (%s)",
		indexName,
		tableName,
		indexType,
		strings.Join(indexFields, ", "),
	)

	if index.Background {
		createSQL = strings.Replace(createSQL, "CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1)
	}

	if err := d.db.WithContext(ctx).Exec(createSQL).Error; err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Store TTL metadata
	if index.IsTTLIndex {
		metadataSQL := `
			CREATE TABLE IF NOT EXISTS index_metadata (
				table_name TEXT NOT NULL,
				index_name TEXT NOT NULL,
				is_ttl BOOLEAN NOT NULL DEFAULT FALSE,
				ttl_seconds INTEGER,
				PRIMARY KEY (table_name, index_name)
			)
		`
		if err := d.db.WithContext(ctx).Exec(metadataSQL).Error; err != nil {
			return fmt.Errorf("failed to create metadata table: %w", err)
		}

		ttlSQL := `
			INSERT INTO index_metadata (table_name, index_name, is_ttl, ttl_seconds)
			VALUES (?, ?, TRUE, ?)
			ON CONFLICT (table_name, index_name) DO UPDATE
			SET is_ttl = TRUE, ttl_seconds = EXCLUDED.ttl_seconds
		`
		if err := d.db.WithContext(ctx).Exec(ttlSQL, tableName, indexName, index.TTL).Error; err != nil {
			return fmt.Errorf("failed to store TTL metadata: %w", err)
		}
	} else if err := d.deleteIndexMetadata(ctx, tableName, indexName); err != nil {
		// A dropped TTL index may have left a metadata row behind under this
		// name; clear it so GetIndexes does not report this plain index as TTL.
		return err
	}

	return nil
}

func getDirectionString(direction any) string {
	switch v := direction.(type) {
	case int:
		if v < 0 {
			return "desc"
		}
	case int32:
		if v < 0 {
			return "desc"
		}
	case int64:
		if v < 0 {
			return "desc"
		}
	case string:
		return v
	}

	return "asc"
}

// GetIndexes retrieves all indexes defined on the table of the given DBObject.
// Returns a slice of indexes or an error if the operation fails.
func (d *driver) GetIndexes(ctx context.Context, row model.DBObject) ([]model.Index, error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return nil, err
	}

	exists, err := d.tableExists(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrorCollectionNotFound
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
            t.relname = ?
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

	for _, idxRow := range rows {
		idx, exists := indexMap[idxRow.IndexName]
		if !exists {
			idx = &model.Index{
				Name:       idxRow.IndexName,
				Background: false,
				Keys:       []model.DBM{},
				IsTTLIndex: false,
				TTL:        0,
			}
			indexMap[idxRow.IndexName] = idx
		}

		idx.Keys = append(idx.Keys, model.DBM{idxRow.ColumnName: idxRow.Direction})
	}

	// TTL metadata only annotates indexes already collected above, so when there
	// are no secondary indexes there is nothing to enrich — skip both round-trips.
	//
	// index_metadata is only created alongside the first TTL index, so its
	// absence just means there is no TTL metadata to report. Any other failure
	// must surface: silently skipping it would report TTL indexes as plain ones.
	if len(indexMap) > 0 {
		metaExists, err := d.tableExists(ctx, "index_metadata")
		if err != nil {
			return nil, err
		}

		if metaExists {
			quotedTable, err := sanitizeIdentifier(tableName)
			if err != nil {
				return nil, err
			}

			type ttlMeta struct {
				IndexName  string `gorm:"column:index_name"`
				TTLSeconds int    `gorm:"column:ttl_seconds"`
			}

			var metas []ttlMeta

			ttlQ := `SELECT index_name, ttl_seconds FROM index_metadata WHERE table_name = ?`
			if err := d.db.WithContext(ctx).Raw(ttlQ, quotedTable).Scan(&metas).Error; err != nil {
				return nil, fmt.Errorf("failed to query TTL index metadata: %w", err)
			}

			for _, m := range metas {
				// pq.QuoteIdentifier wraps names in double-quotes; strip before map lookup.
				key := strings.Trim(m.IndexName, `"`)
				if idx, found := indexMap[key]; found {
					idx.IsTTLIndex = true
					idx.TTL = m.TTLSeconds
				}
			}
		}
	}

	// Convert the map to a slice
	indexes := make([]model.Index, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

func (d *driver) tableExists(ctx context.Context, tableName string) (bool, error) {
	var exists bool

	sql := "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = ?)"

	err := d.db.WithContext(ctx).Raw(sql, tableName).Scan(&exists).Error
	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}

	return exists, nil
}

// deleteIndexMetadata removes TTL metadata rows for the given quoted table
// (and optionally a single quoted index; empty means all of the table's rows)
// so a dropped or recreated index does not inherit TTL attributes from a
// previous incarnation. index_metadata is only created alongside the first
// TTL index, so its absence means there is nothing to clean.
func (d *driver) deleteIndexMetadata(ctx context.Context, quotedTable, quotedIndex string) error {
	exists, err := d.tableExists(ctx, "index_metadata")
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	query := `DELETE FROM index_metadata WHERE table_name = ?`
	args := []interface{}{quotedTable}

	if quotedIndex != "" {
		query += ` AND index_name = ?`

		args = append(args, quotedIndex)
	}

	if err := d.db.WithContext(ctx).Exec(query, args...).Error; err != nil {
		return fmt.Errorf("failed to delete TTL index metadata: %w", err)
	}

	return nil
}

// CleanIndexes removes all non-primary indexes from the table of the given DBObject.
// Returns an error if the cleanup operation fails.
func (d *driver) CleanIndexes(ctx context.Context, row model.DBObject) error {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return err
	}

	exists, err := d.tableExists(ctx, tableName)
	if err != nil {
		return err
	}

	if !exists {
		return ErrorCollectionNotFound
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
            t.relname = ?
            AND c.contype IS NULL  -- Exclude indexes that are part of constraints
            AND NOT ix.indisprimary  -- Exclude primary key indexes
        ORDER BY
            i.relname
    `

	var indexNames []string

	// Execute the query
	err = d.db.WithContext(ctx).Raw(query, tableName).Scan(&indexNames).Error
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

	// The dropped indexes' TTL metadata must go too, otherwise a later index
	// reusing one of these names would be misreported as TTL. A table name
	// that fails sanitization can never have been given TTL metadata by
	// CreateIndex, so there is nothing to clean in that case.
	if quotedTable, qErr := sanitizeIdentifier(tableName); qErr == nil {
		if mErr := d.deleteIndexMetadata(ctx, quotedTable, ""); mErr != nil {
			return mErr
		}
	}

	return nil
}

// Helper function to check if an index exists
func (d *driver) indexExists(ctx context.Context, tableName, indexName string) (bool, error) {
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM pg_indexes 
            WHERE tablename = ?
            AND indexname = ?
        )
    `

	var exists bool

	err := d.db.WithContext(ctx).Raw(query, tableName, indexName).Scan(&exists).Error
	if err != nil {
		return false, err
	}

	return exists, nil
}
