package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/TykTechnologies/storage/persistent/utils"
	"gorm.io/gorm"
	"strconv"
	"time"
)

type BasicStats struct {
	RowCount          int64
	DeadRowCount      int64
	ModifiedCount     int64
	LastVacuum        sql.NullTime
	LastAutovacuum    sql.NullTime
	LastAnalyze       sql.NullTime
	LastAutoanalyze   sql.NullTime
	VacuumCount       int64
	AutovacuumCount   int64
	AnalyzeCount      int64
	AutoanalyzeCount  int64
	EstimatedRowCount float64
	PageCount         float64
	SizeBytes         int64
	TotalSizeBytes    int64
}

// IndexStats Define a struct to hold the index statistics
type IndexStats struct {
	IndexName      string
	ScanCount      int64
	TuplesRead     int64
	TuplesFetched  int64
	IndexSizeBytes int64
}

// ColumnStats Define a struct to hold the column statistics
type ColumnStats struct {
	ColumnName       string
	DataType         string
	NullFraction     float64
	AverageWidth     float64
	DistinctValues   float64
	MostCommonValues sql.NullString
	MostCommonFreqs  sql.NullString
	HistogramBounds  sql.NullString
	Correlation      sql.NullString
}

type BasicInfo struct {
	DBName         string
	DBUser         string
	Version        string
	ServerVersion  string
	DBSizeBytes    int64
	StartTime      time.Time
	MaxConnections string
}

// HasTable checks if a table with the given name exists in the database.
// Returns true if the table exists, otherwise false with any error encountered.
func (d *driver) HasTable(ctx context.Context, tableName string) (bool, error) {
	db := d.readDB
	// Check if the database connection is valid
	if db == nil {
		return false, errors.New(types.ErrorSessionClosed)
	}

	// Validate table name
	if tableName == "" {
		return false, errors.New(types.ErrorEmptyTableName)
	}

	return db.Migrator().HasTable(tableName), nil
}

// DBTableStats returns statistics for the given table.
// Provides details like row count, size, and other metadata as a map.
func (d *driver) DBTableStats(ctx context.Context, row model.DBObject) (model.DBM, error) {
	db := d.readDB
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return nil, err
	}

	exist, err := d.HasTable(ctx, row.TableName())
	if !exist || err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
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
            pg_stat_user_tables.relname = ?
    `
	var basicStats BasicStats

	err = db.WithContext(ctx).Raw(basicStatsQuery, tableName).Scan(&basicStats).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("table %s not found", tableName)
		}
		return nil, fmt.Errorf("failed to get table statistics: %w", err)
	}

	// Add basic statistics to the result map
	stats["row_count"] = basicStats.RowCount
	stats["dead_row_count"] = basicStats.DeadRowCount
	stats["modified_count"] = basicStats.ModifiedCount
	stats["vacuum_count"] = basicStats.VacuumCount
	stats["autovacuum_count"] = basicStats.AutovacuumCount
	stats["analyze_count"] = basicStats.AnalyzeCount
	stats["autoanalyze_count"] = basicStats.AutoanalyzeCount
	stats["estimated_row_count"] = basicStats.EstimatedRowCount
	stats["page_count"] = basicStats.PageCount
	stats["size_bytes"] = basicStats.SizeBytes
	stats["total_size_bytes"] = basicStats.TotalSizeBytes

	// Handle nullable timestamps
	if basicStats.LastVacuum.Valid {
		stats["last_vacuum"] = basicStats.LastVacuum.Time
	} else {
		stats["last_vacuum"] = nil
	}

	if basicStats.LastAutovacuum.Valid {
		stats["last_autovacuum"] = basicStats.LastAutovacuum.Time
	} else {
		stats["last_autovacuum"] = nil
	}

	if basicStats.LastAnalyze.Valid {
		stats["last_analyze"] = basicStats.LastAnalyze.Time
	} else {
		stats["last_analyze"] = nil
	}

	if basicStats.LastAutoanalyze.Valid {
		stats["last_autoanalyze"] = basicStats.LastAutoanalyze.Time
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
            relname = ?
    `

	var indexStatsRows []IndexStats
	err = db.WithContext(ctx).Raw(indexStatsQuery, tableName).Scan(&indexStatsRows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get index statistics: %w", err)
	}

	// Initialize indexes array
	indexes := []model.DBM{}

	// Process each index
	for _, indexStats := range indexStatsRows {
		indexStatsMap := model.DBM{
			"name":           indexStats.IndexName,
			"scan_count":     indexStats.ScanCount,
			"tuples_read":    indexStats.TuplesRead,
			"tuples_fetched": indexStats.TuplesFetched,
			"size_bytes":     indexStats.IndexSizeBytes,
		}

		indexes = append(indexes, indexStatsMap)
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
            LEFT JOIN pg_catalog.pg_stats s ON (s.tablename = ? AND s.attname = a.attname)
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        WHERE
            c.relname = ?
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY
            a.attnum
    `

	var columnStatsRows []ColumnStats
	err = db.WithContext(ctx).Raw(columnStatsQuery, tableName, tableName).Scan(&columnStatsRows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get column statistics: %w", err)
	}

	// Initialize columns array
	columns := []model.DBM{}

	// Process each column
	for _, columnStats := range columnStatsRows {
		columnStatsMap := model.DBM{
			"name":            columnStats.ColumnName,
			"data_type":       columnStats.DataType,
			"null_fraction":   columnStats.NullFraction,
			"average_width":   columnStats.AverageWidth,
			"distinct_values": columnStats.DistinctValues,
		}

		// Handle nullable strings
		if columnStats.MostCommonValues.Valid {
			columnStatsMap["most_common_values"] = columnStats.MostCommonValues.String
		}

		if columnStats.MostCommonFreqs.Valid {
			columnStatsMap["most_common_frequencies"] = columnStats.MostCommonFreqs.String
		}

		if columnStats.HistogramBounds.Valid {
			columnStatsMap["histogram_bounds"] = columnStats.HistogramBounds.String
		}

		if columnStats.Correlation.Valid {
			columnStatsMap["correlation"] = columnStats.Correlation.String
		}

		columns = append(columns, columnStatsMap)
	}

	// Add columns to the result map
	stats["columns"] = columns
	// Add MongoDB-compatible fields
	stats["ns"] = tableName                          // Namespace (table name in PostgreSQL)
	stats["count"] = basicStats.RowCount             // Document count
	stats["size"] = basicStats.SizeBytes             // Size in bytes
	stats["storageSize"] = basicStats.TotalSizeBytes // Total storage size
	stats["capped"] = false                          // PostgreSQL doesn't have capped collections
	stats["wiredTiger"] = nil                        // PostgreSQL doesn't use WiredTiger

	return stats, nil
}

// GetTables retrieves the list of all table names in the current database.
// Returns a slice of table names or an error if the query fails.
func (d *driver) GetTables(ctx context.Context) ([]string, error) {
	db := d.readDB
	if db == nil {
		return []string{}, errors.New(types.ErrorSessionClosed)
	}

	var tables []string
	err := db.Raw(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`).Scan(&tables).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	return tables, nil
}

// DropTable removes a table by its name.
// Returns the number of tables dropped and any error encountered.
func (d *driver) DropTable(ctx context.Context, name string) (int, error) {
	writeDB := d.writeDB
	readDB := d.readDB
	if db == nil {
		return 0, errors.New(types.ErrorSessionClosed)
	}

	// Validate table name
	if name == "" {
		return 0, errors.New(types.ErrorEmptyTableName)
	}

	exist, err := d.HasTable(ctx, name)
	if err != nil || !exist {
		return 0, err
	}

	// Get the number of rows in the table before dropping it
	// This is to return the number of affected rows
	var rowCount int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", name)
	err = readDB.WithContext(ctx).Raw(countQuery).Scan(&rowCount).Error
	if err != nil {
		return 0, err
	}

	err = writeDB.Migrator().DropTable(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("failed to drop table %s: %w", name, err)
	}
	return int(rowCount), nil
}

// Drop removes the table associated with the given object.
// Permanently deletes its schema and all stored data.
func (d *driver) Drop(ctx context.Context, object model.DBObject) error {
	db := d.writeDB
	// Check if the database connection is valid
	tableName, err := d.validateDBAndTable(object)
	if err != nil {
		return err
	}

	err = db.WithContext(ctx).Migrator().DropTable(tableName)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	return nil
}

// Migrate applies schema changes for the given objects.
// Ensures tables and indexes are created or updated based on the models.
func (d *driver) Migrate(ctx context.Context, objects []model.DBObject, options ...model.DBM) error {
	// Check if the database connection is valid
	if d.writeDB == nil {
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

	// Use GORM's context
	writeDB := d.writeDB.WithContext(ctx)
	// Process each object
	for _, obj := range objects {
		// Get the table name
		if obj == nil {
			return errors.New(types.ErrorNilObject)
		}
		tableName := obj.TableName()
		if tableName == "" {
			return errors.New(types.ErrorEmptyTableName)
		}

		// Check if the table already exists
		tableExists := writeDB.Migrator().HasTable(tableName)
		if tableExists {
			continue // Skip if table already exists
		}

		err := writeDB.Table(tableName).AutoMigrate(obj)
		if err != nil {
			return fmt.Errorf("failed to migrate table %s: %w", tableName, err)
		}

		// Apply any additional options (like indexes) that aren't handled by AutoMigrate
		if len(options) == 1 {
			opts := options[0]
			// Check for indexes to create
			if indexes, ok := opts["indexes"].([]model.Index); ok {
				for _, index := range indexes {
					// Skip if index is empty
					if len(index.Keys) == 0 {
						continue
					}

					// Create the index using our existing CreateIndex function
					err = d.CreateIndex(ctx, obj, index)
					if err != nil {
						// If the error is just that the index already exists, we can continue
						if err.Error() == types.ErrorIndexAlreadyExist {
							continue
						}
						return fmt.Errorf("failed to create index %s on table %s: %w", index.Name, tableName, err)
					}
				}
			}
		}
	}

	return nil
}

// GetDatabaseInfo returns metadata about the connected database.
// Provides details such as type, version, and connection information.
func (d *driver) GetDatabaseInfo(ctx context.Context) (utils.Info, error) {
	readDB := d.readDB
	if readDB == nil {
		return utils.Info{}, errors.New(types.ErrorSessionClosed)
	}

	// Initialize the result structure
	info := utils.Info{
		Type: utils.PostgresDB, // Assuming utils.PostgresSQL is defined in the utils package
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
	var basicInfo BasicInfo
	err := readDB.WithContext(ctx).Raw(basicInfoQuery).Scan(&basicInfo).Error
	if err != nil {
		return utils.Info{}, fmt.Errorf("failed to get database info: %w", err)
	}

	info.Name = basicInfo.DBName
	info.User = basicInfo.DBUser
	info.Version = basicInfo.ServerVersion
	info.FullVersion = basicInfo.Version
	info.SizeBytes = basicInfo.DBSizeBytes
	info.StartTime = basicInfo.StartTime

	// Convert max_connections to int
	maxConn, err := strconv.Atoi(basicInfo.MaxConnections)
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
	err = readDB.WithContext(ctx).Raw(connectionCountQuery).Scan(&connectionCount).Error
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
	err = readDB.WithContext(ctx).Raw(tableCountQuery).Scan(&tableCount).Error
	if err == nil {
		info.TableCount = tableCount
	}

	return info, nil
}
