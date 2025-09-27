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
	db               *gorm.DB
	connectionString string
	dbName           string
	sqlDB            *sql.DB
}

// Connect initializes a new database connection using the provided client options.
// Returns an error if the connection cannot be established.
func (l *lifeCycle) Connect(opts *types.ClientOpts) error {
	if opts == nil {
		return errors.New("nil opts")
	}

	// Use the connection string exactly as provided
	dsn := strings.TrimSpace(opts.ConnectionString)
	if dsn == "" {
		return errors.New("empty connection string")
	}

	// Open GORM with PostgreSQL driver
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("gorm open: %w", err)
	}
	l.db = db

	// Get underlying sql.DB
	sqlDB, err := db.DB()
	if err != nil {
		l.db = nil
		return fmt.Errorf("getting sql.DB: %w", err)
	}
	l.sqlDB = sqlDB

	// Set connection lifetime if provided
	if opts.ConnectionTimeout > 0 {
		l.sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	// Ping to verify connection
	pingTimeout := 5 * time.Second
	if opts.ConnectionTimeout > 0 {
		pingTimeout = time.Duration(opts.ConnectionTimeout) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := l.sqlDB.PingContext(ctx); err != nil {
		l.db = nil
		return fmt.Errorf("ping db: %w", err)
	}

	// Extract database name (supports both DSN and URL formats)
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

	return nil
}

// Close terminates the active database connection.
// Returns an error if the connection cannot be closed properly.
func (l *lifeCycle) Close() error {
	err := l.sqlDB.Close()
	if err != nil {
		return err
	}
	l.db = nil
	return nil
}

// DBType returns the type of database managed by this lifecycle.
// Useful for distinguishing between supported database backends.
func (l *lifeCycle) DBType() utils.DBType {
	return utils.PostgresDB
}

// Ping checks the health of the database connection.
// Returns an error if the database is unreachable or not responding.
func (d *driver) Ping(ctx context.Context) error {
	// Check if the database connection is valid
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	if ctx == nil {
		return errors.New(types.ErrorNilContext)
	}

	// Get the underlying *sql.DB from GORM
	sqlDB, err := d.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	// Use the standard library's PingContext method
	err = sqlDB.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

// DropDatabase removes the connected database entirely.
// Returns an error if the operation fails.
func (d *driver) DropDatabase(_ context.Context) error {
	// Check if the database connection is valid
	if d.db == nil {
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

	// Set the driver's db to nil to prevent further use
	d.db = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

var _ types.StorageLifecycle = &lifeCycle{}
