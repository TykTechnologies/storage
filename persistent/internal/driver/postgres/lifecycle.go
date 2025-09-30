package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/utils"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"net/url"
	"strings"
	"time"
)

type lifeCycle struct {
	writeDB *gorm.DB
	readDB  *gorm.DB

	dbName     string
	writeSQLDB *sql.DB
	readSQLDB  *sql.DB
}

func (l *lifeCycle) establishConnection(dsn string, opts *types.ClientOpts) (*gorm.DB, *sql.DB, error) {
	// Create dialect from DSN
	dialect := postgres.New(postgres.Config{
		DSN: dsn,
	})

	// Open connection
	db, err := gorm.Open(dialect, &gorm.Config{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Get underlying SQL DB
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Configure connection
	if opts.ConnectionTimeout > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	// Ping database to verify connection
	pingTimeout := 5 * time.Second
	if opts.ConnectionTimeout > 0 {
		pingTimeout = time.Duration(opts.ConnectionTimeout) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		err := sqlDB.Close()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("database ping failed: %w", err)
	}

	return db, sqlDB, nil
}

// Connect initializes a new database connection using the provided client options.
// Returns an error if the connection cannot be established.
func (l *lifeCycle) Connect(opts *types.ClientOpts) error {
	if opts == nil {
		return errors.New("nil opts")
	}

	writeDSN := opts.ConnectionString
	readDSN := opts.ReadConnectionString

	// Validate connection strings
	if writeDSN == "" {
		return errors.New("write connection string is required")
	}

	// If no separate read connection specified, use the write connection for reads
	if readDSN == "" {
		readDSN = writeDSN
	}

	// Establish write connection
	var err error
	l.writeDB, l.writeSQLDB, err = l.establishConnection(writeDSN, opts)
	if err != nil {
		return fmt.Errorf("failed to establish write connection: %w", err)
	}

	// Extract database name from the write connection
	l.extractDBName(writeDSN)

	// Handle read connection
	if readDSN == writeDSN {
		// Use write connection for reads if they're the same
		l.readDB = l.writeDB
		l.readSQLDB = l.writeSQLDB
	} else {
		// Establish separate read connection
		l.readDB, l.readSQLDB, err = l.establishConnection(readDSN, opts)
		if err != nil {
			// Clean up write connection
			err := l.writeSQLDB.Close()
			if err != nil {
				return errors.New("failed to close write database")
			}
			l.writeDB = nil
			l.writeSQLDB = nil
			return errors.New("failed to establish read connection")
		}
	}
	return nil
}

func (l *lifeCycle) closeWriteConnection() error {
	if l.writeSQLDB != nil {
		err := l.writeSQLDB.Close()
		if err != nil {
			return errors.New("failed to close write connection")
		}
		l.writeDB = nil
		l.writeSQLDB = nil
	}
	return nil
}

func (l *lifeCycle) extractDBName(dsn string) {
	if parts := strings.Fields(dsn); len(parts) > 0 {
		for _, part := range parts {
			if strings.HasPrefix(part, "dbname=") {
				l.dbName = strings.TrimPrefix(part, "dbname=")
				break
			}
		}
	}
	if l.dbName == "" && (strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")) {
		if u, err := url.Parse(dsn); err == nil {
			l.dbName = strings.TrimPrefix(u.Path, "/")
		}
	}
}

// Close terminates the active database connection.
// Returns an error if the connection cannot be closed properly.
func (l *lifeCycle) Close() error {
	var err error
	connectionClosed := false

	// Close write connection
	// Close write connection
	if l.writeSQLDB != nil {
		err = l.writeSQLDB.Close()
		connectionClosed = true
		l.writeDB = nil
		l.writeSQLDB = nil
	}

	// Only close read connection if it's different from write connection
	// or if we haven't closed any connection yet
	if l.readSQLDB != nil && (!connectionClosed || l.readSQLDB != l.writeSQLDB) {
		readErr := l.readSQLDB.Close()
		// If we didn't have an error from closing write connection,
		// use the error from closing read connection
		if err == nil {
			err = readErr
		}
	}

	// Always nil the read pointers
	l.readDB = nil
	l.readSQLDB = nil

	return err
}

// DBType returns the type of database managed by this lifecycle.
// Useful for distinguishing between supported database backends.
func (l *lifeCycle) DBType() utils.DBType {
	return utils.PostgresDB
}

// Ping checks the health of the database connection.
// Returns an error if the database is unreachable or not responding.
func (d *driver) Ping(ctx context.Context) error {
	// Check if connections exist
	if d.writeDB == nil || d.readDB == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	if ctx == nil {
		return errors.New(types.ErrorNilContext)
	}

	// Ping write connection
	writeSQLDB, err := d.writeDB.DB()
	if err != nil {
		return fmt.Errorf("failed to get write database connection: %w", err)
	}

	if err := writeSQLDB.PingContext(ctx); err != nil {
		return fmt.Errorf("write database ping failed: %w", err)
	}

	// If read and write are the same connection, we're done
	if d.readDB == d.writeDB {
		return nil
	}

	// Ping read connection
	readSQLDB, err := d.readDB.DB()
	if err != nil {
		return fmt.Errorf("failed to get read database connection: %w", err)
	}

	if err := readSQLDB.PingContext(ctx); err != nil {
		return fmt.Errorf("read database ping failed: %w", err)
	}

	return nil
}

// DropDatabase removes the connected database entirely.
// Returns an error if the operation fails.
func (d *driver) DropDatabase(_ context.Context) error {
	// Check if the database connection is valid
	if d.writeDB == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	dbNameToDelete := d.dbName

	// In PostgresSQL, we cannot drop the currently connected database.
	// We need to provide instructions on how to drop the database manually.

	// Close the current connection
	err := d.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	// Set the driver's writeDB to nil to prevent further use
	d.writeDB = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

var _ types.StorageLifecycle = &lifeCycle{}
